while tonumber(ARGV[1]) >= 1 do
    redis.call("get", KEYS[1])
    print("I am in a loop, use SCRIPT_KILL")
end
