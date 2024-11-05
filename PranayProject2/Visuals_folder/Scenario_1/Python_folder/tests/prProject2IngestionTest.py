import pytest
from unittest import mock
from unittest.mock import MagicMock
import pandas as pd
from botocore.exceptions import ClientError

# Import your lambda function module
import prProject2Ingestion

@pytest.fixture
def mock_environment_variables(monkeypatch):
    monkeypatch.setenv("JDBC_HOSTNAME", "test_host")
    monkeypatch.setenv("JDBC_PORT", "5432")
    monkeypatch.setenv("JDBC_DATABASE", "test_db")
    monkeypatch.setenv("JDBC_USERNAME", "test_user")
    monkeypatch.setenv("JDBC_PASSWORD", "test_password")
    monkeypatch.setenv("S3_BUCKET", "test_bucket")


@pytest.fixture
def mock_postgresql_data():
    data = {
        "review_id": [1, 2],
        "review_text": ["Good product", "Could be better"],
        "review_date": ["2023-10-01", "2023-10-02"],
        "review_timestamp": ["2023-10-01 12:34:56", "2023-10-02 15:45:23"]
    }
    return pd.DataFrame(data)


@mock.patch("prProject2Ingestion.psycopg2.connect")
@mock.patch("prProject2Ingestion.boto3.client")
def test_lambda_handler_success(mock_boto3_client, mock_psycopg2_connect, mock_environment_variables,
                                mock_postgresql_data):
    # Mock the PostgreSQL connection and data fetching
    mock_conn = mock.MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor().fetchall.return_value = mock_postgresql_data

    # Mock pandas.read_sql to return the mock data
    with mock.patch("prProject2Ingestion.pd.read_sql", return_value=mock_postgresql_data):
        # Mock the S3 client and its put_object method
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Call the Lambda handler
        response = prProject2Ingestion.lambda_handler({}, {})

        # Check the response
        assert response['statusCode'] == 200
        assert "Data from PostgreSQL has been successfully loaded to S3 in Parquet format." in response['body']

        # Check that the S3 client was called
        mock_s3_client.put_object.assert_called_once()

        # Verify file upload parameters
        args, kwargs = mock_s3_client.put_object.call_args
        assert kwargs['Bucket'] == "test_bucket"
        assert "prProject2RawBronze/prProject2ToProcess/amazon_reviews_" in kwargs['Key']
        assert isinstance(kwargs['Body'], bytes)


@mock.patch("prProject2Ingestion.psycopg2.connect")
@mock.patch("prProject2Ingestion.boto3.client")
def test_lambda_handler_s3_upload_failure(mock_boto3_client, mock_psycopg2_connect, mock_environment_variables,
                                          mock_postgresql_data):
    # Mock the PostgreSQL connection and data fetching
    mock_conn = mock.MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor().fetchall.return_value = mock_postgresql_data

    # Mock pandas.read_sql to return the mock data
    with mock.patch("prProject2Ingestion.pd.read_sql", return_value=mock_postgresql_data):
        # Simulate an S3 upload failure
        mock_s3_client = MagicMock()
        mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Internal Server Error"}},
            "put_object"
        )
        mock_boto3_client.return_value = mock_s3_client

        # Call the Lambda handler and expect an exception
        with pytest.raises(ClientError):
            prProject2Ingestion.lambda_handler({}, {})

# Run pytest in your terminal to execute these tests
