/* citus--7.4-2--7.4-3 */

CREATE FUNCTION citus.mitmproxy(text) RETURNS text AS $$
DECLARE
  command ALIAS FOR $1;
  result text;
BEGIN
  CREATE TEMPORARY TABLE mitmproxy_command (command text);
  CREATE TEMPORARY TABLE mitmproxy_result (res text);

  INSERT INTO mitmproxy_command VALUES (command);

  EXECUTE format('COPY mitmproxy_command TO %L', current_setting('citus.mitmfifo'));
  EXECUTE format('COPY mitmproxy_result FROM %L', current_setting('citus.mitmfifo'));

  SELECT res INTO result FROM mitmproxy_result;

  DROP TABLE mitmproxy_command;
  DROP TABLE mitmproxy_result;

  RETURN result;
END;
$$ LANGUAGE plpgsql;

