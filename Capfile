# Docs at http://confluence.local.twitter.com/display/RELEASE/Twitter-cap-utils+README
begin
  require 'rubygems'
  gem 'twitter-cap-utils', "0.4.2"
  require "railsless-deploy"
  require 'twitter_cap_utils'
rescue LoadError
  abort "Please gem install twitter-cap-utils railsless-deploy"
end

set :application, "haplo"
set :repository, "http://git.local.twitter.com/ro/haplocheirus"
set :package_name, "haplocheirus-1.0.zip"

# FIXME: push most of this into cap
BUILD_HOST_BIN_FOLDER = "/home/twitter/bin"
SBT = File.exists?("#{BUILD_HOST_BIN_FOLDER}/sbt") ? "#{BUILD_HOST_BIN_FOLDER}/sbt" : "sbt"
THRIFT_BIN = File.exists?("#{BUILD_HOST_BIN_FOLDER}/thrift-0.2.0") ? "#{BUILD_HOST_BIN_FOLDER}/thrift-0.2.0" : "thrift"
set :build_command, "env NO_TESTS=1 SBT_TWITTER=1 THRIFT_BIN=#{THRIFT_BIN} #{SBT} update package-dist"

task :production do
  role :haplo, "sjc1h108.prod.twitter.com"
end

task :canary do
  role :haplo, "sjc1h108.prod.twitter.com"
end

task :bootstrap do
  # FFS THIS IS TERRIBLE
  STDERR.print "SUDO Password: "
  old_state = `stty -g`
  system "stty -echo"
  sudo_password = STDIN.gets
  system "stty #{old_state}"
  puts

  set :user, ENV["USER"]

  commands = [
    "mkdir -p /usr/local/#{application}/releases",
    "chown -R twitter:twitter /usr/local/#{application}",
    "mkdir -p /var/log/haplo",
    "chown twitter:twitter /var/log/haplo",
  ]

  run "sudo sh -c '#{commands.join(' && ')}'" do |channel, stream, data|
    channel.send_data(sudo_password)
  end
end

# after "deploy:symlink" do
#   run "chmod +x #{current_path}/scripts/flock.sh"
# end
# 
# after "deploy:subrestart" do
#   sleep 60
# end
