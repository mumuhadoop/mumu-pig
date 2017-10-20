password = load '/etc/passwd' using PigStorage(':');

username_password = foreach password generate $0 as username;