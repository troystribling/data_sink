require 'aws-sdk'
require 'json'
require 'date'

default_date = Date.today.strftime('%Y%m%d')
start_date = Date.parse(ARGV[0] || default_date)
end_date = Date.parse(ARGV[1] || default_date)

s3 = Aws::S3::Resource.new
bucket = s3.bucket('gly.fish')

(start_date..end_date).each do |date|
  tweets_folder = "tweets/#{date.strftime('%Y%m%d')}"
  puts "Downloading #{tweets_folder}"
  remote_files = bucket.objects(prefix: tweets_folder)
  remote_files.each do |remote_file|
    local_file_name = "/tmp/#{remote_file.key.split(File::SEPARATOR).last}"
    File.open(local_file_name, 'wb') do |local_file|
      local_file.write(remote_file.get.body.read)
    end
    `lzop -d #{local_file_name}`
    File.delete(local_file_name)
  end
end
