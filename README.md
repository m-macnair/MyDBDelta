# MyDBDelta
System for creating mysql/mariadb dumps, discarding identical duplicates over time

# Usage

script/mk2.pl -cfg <configuration.json>

# Config
Configuration is specified in json format

## required   
db - name of db

host - ipaddress or localhost

user - db user

pass - db pass

path - path to dump out resources 


## optional

driver - defaults to mysql, used in the dbi connection 

gitmode - assuming path is to a git repo, git add, git commit and git push 

skipdelta - only write one file, best used in combination with gitmode


# Dependencies

github.com/m-macnair/Toolbox

github.com/m-macnair/Moo-Role
