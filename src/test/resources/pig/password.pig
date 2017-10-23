password = load '/pig/passwd' using PigStorage(':');

username_password = foreach password generate $0 as username;

dump username_password;