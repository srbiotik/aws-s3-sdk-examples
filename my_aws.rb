# frozen_string_literal: true

require 'dotenv'
require 'aws-sdk-s3'

Dotenv.load

# This class is responsible for uploading files to S3
class MyAWS
  def initialize
    Aws.config.update(
      {
        access_key_id: ENV['ACCESS_KEY_ID'],
        secret_access_key: ENV['SECRET_ACCESS_KEY'],
        region: ENV['REGION']
      }
    )
    @bucket = ENV['AWS_BUCKET']
    @client = Aws::S3::Client.new
    @part_size = 5 * 1024 * 1024 # 5 MB
    @parts = []
  end

  # List Bucket Objects
  def list
    @client.list_objects(bucket: ENV['AWS_BUCKET'])
  end

  # Uploads a file to S3
  def put(file_name, key)
    File.open(file_name, 'rb') do |file_data|
      # Upload a file
      @client.put_object(bucket: ENV['AWS_BUCKET'], key:, body: file_data)
    end
  end

  # Uploads a file to S3
  def read(key)
    @client.get_object(bucket: ENV['AWS_BUCKET'], key:)
  end

  # Implements multi-part file upload, which is a three step process:
  # Create the multi-part upload and get an upload ID
  # Break down the upload file into smaller chunks and upload each chunk, storing the returned ETag for each chunk
  # Complete the multi-part upload using the upload ID and ETags to identify the chunks that compose the file
  # @param [String] file_name
  # @param [String] key
  # @raise [Aws::S3::Errors::ServiceError]
  # @return [void]
  def multi_part_upload(file_name, key)
    # TODO: Check if bucket has permissions to upload in multi-part
    bucket = ENV['AWS_BUCKET']
    multipart_upload = @client.create_multipart_upload(bucket:, key:)
    upload_id = multipart_upload.upload_id

    File.open(file_name, 'rb') do |file_data|
      part_number = 1
      while (body = file_data.read(@part_size))
        response = @client.upload_part({ bucket:, key:, part_number:, upload_id:, body: })
        etag = response.etag
        @parts << { etag:, part_number: }
        puts "Uploaded part #{part_number} with etag #{etag}"
        part_number += 1
      end
    end

    complete_multipart_upload_response = @client.complete_multipart_upload(
      bucket:,
      upload_id:,
      key:,
      multipart_upload: {
        parts: @parts
      }
    )
    puts complete_multipart_upload_response
  end
end
