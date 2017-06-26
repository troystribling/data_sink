require 'tweetstream'
require 'yaml'

credentials = YAML.load_file(File.join([ENV['HOME'], '.twitter/credentials.yml']))

TweetStream.configure do |config|
  config.consumer_key = credentials['consumer_key']
  config.consumer_secret = credentials['consumer_secret']
  config.oauth_token  = credentials['oauth_token']
  config.oauth_token_secret = credentials['oauth_token_secret']
  config.auth_method = :oauth
end

client = TweetStream::Client.new

client.on_error do |message|
  puts message
end

search_term = ARGV[0] || 'trump'
puts "tracking: #{search_term}"

client.track(search_term) do |status|
  puts "#{status.attrs}"
end
