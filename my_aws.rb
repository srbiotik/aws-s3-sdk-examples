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
    @client = Aws::S3::Client.new
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

  # Implements multi-part file upload
  # @param [String] file
  # @param [String] key
  # @raise [Aws::S3::Errors::ServiceError]
  # @return [void]
  def multi_part_upload(file, key)
    create_multipart_upload_response = @client.create_multipart_upload(ENV['AWS_BUCKET'], key:)
    upload_id = create_multipart_upload_response.upload_id

    File.open(file, 'rb') do |file_data|
      part_number = 1
      while (body = file_data.read(5 * 1024 * 1024))
        @client.upload_part(
          bucket: ENV['AWS_BUCKET'],
          key:,
          part_number:,
          upload_id:,
          body:
        )
        part_number += 1
      end
    end

    complete_multipart_upload_response = @client.complete_multipart_upload(
      bucket: ENV['AWS_BUCKET'],
      upload_id:,
      key:,
      multipart_upload: {
        parts: create_part_list(upload_id)
      }
    )
  end

  private

  def create_part_list(upload_id)
    list_parts_response = @client.list_parts(
      bucket: ENV['AWS_BUCKET'],
      key:,
      upload_id:
    )
    list_parts_response.parts.map(&:part_number)
  end
end
