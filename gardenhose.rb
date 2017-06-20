require 'tweetstream'
require 'aws-sdk'
require 'tempfile'
require 'json'
require 'yaml'

credentials = YAML.load_file(File.join([ENV['HOME'], '.twitter/credentials.yml']))

TweetStream.configure do |config|
  config.consumer_key = credentials['consumer_key']
  config.consumer_secret = credentials['consumer_secret']
  config.oauth_token  = credentials['oauth_token']
  config.oauth_token_secret = credentials['oauth_token_secret']
  config.auth_method = :oauth
end

tweets_per_file = (ARGV[0]|| 1000).to_i
tweets = []
count = 0

@tweets_file_queue = Queue.new

def write_tweets
  EM.defer do
    data = @tweets_file_queue.pop
    file = Tempfile.new
    data.each do |tweet|
      file.puts(tweet.to_json)
    end
    data = nil
    file.close()
    `lzop #{file.path}`
    lzo_file = "#{file.path}.lzo"
    upload_tweets(lzo_file)
    File.delete(lzo_file)
  end
end

def upload_tweets(file_path)
  file = file_path.split(File::SEPARATOR).last
  date_dir = Date.today.strftime('%Y%m%d')
  s3 = Aws::S3::Resource.new
  s3_file = s3.bucket('gly.fish').object("tweets/#{date_dir}/#{file}")
  s3_file.upload_file(file_path)
end

EM.run do

  client = TweetStream::Client.new

  client.on_error do |message|
    puts message
  end

  client.sample do |status|
    count += 1
    tweets.push(status.attrs)
    if count % tweets_per_file == 0
      @tweets_file_queue.push(tweets)
      tweets = []
      write_tweets
    end
  end

end
