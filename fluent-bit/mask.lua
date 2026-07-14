-- Sensitive-field masking, demonstrated on user_id.
--
-- In a real fleet you'd mask PII (emails, card numbers, tokens) at the edge,
-- before logs ever leave the host, so raw sensitive data never lands in Kafka
-- or Elasticsearch. Here we redact the digits of user_id (user-33554 ->
-- user-*****) to prove the pattern without losing that "a user id was present".
--
-- Fluent Bit Lua filter contract: return code, timestamp, record.
--   code 2  -> record was modified (use the returned record)
function mask_fields(tag, timestamp, record)
    if record["user_id"] ~= nil then
        record["user_id"] = string.gsub(tostring(record["user_id"]), "%d", "*")
    end
    return 2, timestamp, record
end
