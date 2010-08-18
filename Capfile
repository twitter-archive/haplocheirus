# Docs at http://confluence.local.twitter.com/display/RELEASE/Twitter-cap-utils+README
begin
  require 'rubygems'
  gem 'twitter-cap-utils', "0.6.2"
  require "railsless-deploy"
  require 'twitter_cap_utils'
rescue LoadError
  abort "Please gem install twitter-cap-utils railsless-deploy"
end

set :application, "haplocheirus"
set :admin_port, 7667


task :production do
  role :haplo, "sjc1h108.prod.twitter.com"
end

task :canary do
  role :haplo, "sjc1h108.prod.twitter.com"
end

namespace :deploy do
  task :setup do
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
      "mkdir -p /var/log/#{application}",
      "chown daemon:daemon /var/log/#{application}",
      "mkdir -p /var/spool/kestrel",
      "chown daemon:daemon /var/spool/kestrel",
    ]

    run "sudo sh -c '#{commands.join(' && ')}'" do |channel, stream, data|
      channel.send_data(sudo_password)
    end
  end
end

namespace :deploy do
  task :asme do
    set :user, ENV["USER"]
  end

  task :subrestart do
    sudo "/usr/local/#{application}/current/scripts/#{application}.sh restart"
  end
end

after "deploy:subrestart" do
  sleep 10
  deploy.verify_build
end
