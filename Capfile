# Docs at http://confluence.local.twitter.com/display/RELEASE/Twitter-cap-utils+README
begin
  require 'rubygems'
  gem 'twitter-cap-utils', "~> 0.6.8"
  require "railsless-deploy"
  require 'twitter_cap_utils'
rescue LoadError
  abort "Please gem install twitter-cap-utils railsless-deploy"
end

set :user, "haplo"
set :build_user, "twitter"

set :application, "haplocheirus"
set :monit_application, "haplo"
set :admin_port, 7667


task :production do
  role :haplo, *loony_or_override("-g role:haplo")
end

task :canary do
  role :haplo, "sjc1r052.prod.twitter.com"
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
      "chown -R haplo:haplo /usr/local/#{application}",
      "mkdir -p /var/log/#{application}",
      "chown -R haplo:haplo /var/log/#{application}",
      "mkdir -p /var/spool/kestrel",
      "chown -R haplo:haplo /var/spool/kestrel",
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

#  task :subrestart do
#    sudo "/usr/local/#{application}/current/scripts/#{application}.sh restart"
#  end
end

after "deploy:subrestart" do
  set :rolling_restart_group_size, 5
  sleep 30
  execute_with_hosts("deploy:verify_build", find_task("deploy:subrestart").options[:hosts])
end

