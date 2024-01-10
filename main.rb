# frozen_string_literal: true

require './my_aws'
require 'dotenv'
Dotenv.load

$aws = MyAWS.new
