--------------------------------------------------------------------------------
-- HTTP LOG --------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.write_to_log (
  pPath         text,
  pHeaders      jsonb,
  pParams       jsonb DEFAULT null,
  pBody         jsonb DEFAULT null,
  pMethod       text DEFAULT 'GET',
  pMessage      text DEFAULT null,
  pContext      text DEFAULT null
) RETURNS       bigint
AS $$
DECLARE
  nId           bigint;
BEGIN
  INSERT INTO http.log (method, path, headers, params, body, message, context)
  VALUES (pMethod, pPath, pHeaders, pParams, pBody, pMessage, pContext)
  RETURNING id INTO nId;

  RETURN nId;
END;
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP REQUEST ----------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.create_request (
  pResource     text,
  pType         text DEFAULT null,
  pMethod       text DEFAULT 'GET',
  pHeaders      jsonb DEFAULT null,
  pContent      text DEFAULT null,
  pDone         text DEFAULT null,
  pFail         text DEFAULT null,
  pAgent        text DEFAULT null,
  pProfile      text DEFAULT null,
  pCommand      text DEFAULT null,
  pMessage      text DEFAULT null,
  pData         jsonb DEFAULT null
) RETURNS       uuid
AS $$
DECLARE
  uId           uuid;
BEGIN
  INSERT INTO http.request (state, type, method, resource, headers, content, done, fail, agent, profile, command, message, data)
  VALUES (1, coalesce(pType, 'native'), pMethod, pResource, pHeaders, pContent, pDone, pFail, pAgent, pProfile, pCommand, pMessage, pData)
  RETURNING id INTO uId;

  RETURN uId;
END;
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP RESPONSE ---------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.create_response (
  pRequest      uuid,
  pStatus       integer,
  pStatusText   text,
  pHeaders      jsonb,
  pContent      text DEFAULT null
) RETURNS       uuid
AS $$
DECLARE
  uId           uuid;
  cBegin        timestamptz;
BEGIN
  SELECT datetime INTO cBegin FROM http.request WHERE id = pRequest;

  INSERT INTO http.response (id, request, status, status_text, headers, content, runtime)
  VALUES (pRequest, pRequest, pStatus, pStatusText, pHeaders, pContent, age(clock_timestamp(), cBegin))
  RETURNING id INTO uId;

  PERFORM http.done(pRequest);

  RETURN uId;
END;
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP REQUEST ----------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.request (
  pId       uuid
) RETURNS   SETOF http.request
AS $$
  SELECT * FROM http.request WHERE id = pId;
$$ LANGUAGE SQL
   SECURITY DEFINER
   SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP GET --------------------------------------------------------------------
--------------------------------------------------------------------------------
/**
 * Обрабатывает GET запрос.
 * @param {text} path - Путь
 * @param {jsonb} headers - HTTP заголовки
 * @param {jsonb} params - Параметры запроса
 * @return {SETOF json}
 */
CREATE OR REPLACE FUNCTION http.get (
  path      text,
  headers   jsonb,
  params    jsonb DEFAULT null
) RETURNS   SETOF json
AS $$
DECLARE
  r         record;

  nId       bigint;

  cBegin    timestamptz;

  vMessage  text;
  vContext  text;
BEGIN
  nId := http.write_to_log(path, headers, params);

  IF split_part(path, '/', 3) != 'v1' THEN
    RAISE EXCEPTION 'Invalid API version.';
  END IF;

  cBegin := clock_timestamp();

  FOR r IN SELECT * FROM jsonb_each(headers)
  LOOP
    -- parse headers here
  END LOOP;

  CASE split_part(path, '/', 4)
  WHEN 'ping' THEN

    RETURN NEXT json_build_object('code', 200, 'message', 'OK');

  WHEN 'time' THEN

    RETURN NEXT json_build_object('serverTime', trunc(extract(EPOCH FROM Now())));

  WHEN 'webhook' THEN

    RETURN NEXT http.webhook('GET', path, headers, params);

  WHEN 'headers' THEN

	RETURN NEXT coalesce(headers, jsonb_build_object());

  WHEN 'params' THEN

	RETURN NEXT coalesce(params, jsonb_build_object());

  WHEN 'log' THEN

	FOR r IN SELECT * FROM http.log ORDER BY id DESC
    LOOP
	  RETURN NEXT row_to_json(r);
    END LOOP;

  ELSE

    RETURN NEXT json_build_object('error', json_build_object('code', 404, 'message', format('Patch "%s" not found.', path)));

  END CASE;

  UPDATE http.log SET runtime = age(clock_timestamp(), cBegin) WHERE id = nId;

  RETURN;
EXCEPTION
WHEN others THEN
  GET STACKED DIAGNOSTICS vMessage = MESSAGE_TEXT, vContext = PG_EXCEPTION_CONTEXT;

  PERFORM http.write_to_log(path, headers, params, null, 'GET', vMessage, vContext);

  RETURN NEXT json_build_object('error', json_build_object('code', 500, 'message', vMessage));

  RETURN;
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP POST -------------------------------------------------------------------
--------------------------------------------------------------------------------
/**
 * Обрабатывает POST запрос.
 * @param {text} path - Путь
 * @param {jsonb} headers - HTTP заголовки
 * @param {jsonb} params - Параметры запроса
 * @param {jsonb} body - Тело запроса
 * @return {SETOF json}
 */
CREATE OR REPLACE FUNCTION http.post (
  path      text,
  headers   jsonb,
  params    jsonb DEFAULT null,
  body      jsonb DEFAULT null
) RETURNS   SETOF json
AS $$
DECLARE
  nId       bigint;

  cBegin    timestamptz;

  vMessage  text;
  vContext  text;
BEGIN
  nId := http.write_to_log(path, headers, params, body, 'POST');

  IF split_part(path, '/', 3) != 'v1' THEN
    RAISE EXCEPTION 'Invalid API version.';
  END IF;

  cBegin := clock_timestamp();

  CASE split_part(path, '/', 4)
  WHEN 'ping' THEN

    RETURN NEXT json_build_object('code', 200, 'message', 'OK');

  WHEN 'time' THEN

    RETURN NEXT json_build_object('serverTime', trunc(extract(EPOCH FROM Now())));

  WHEN 'headers' THEN

	RETURN NEXT coalesce(headers, jsonb_build_object());

  WHEN 'params' THEN

	RETURN NEXT coalesce(params, jsonb_build_object());

  WHEN 'body' THEN

	RETURN NEXT coalesce(body, jsonb_build_object());

  WHEN 'webhook' THEN

    RETURN NEXT http.webhook('POST', path, headers, params, body);

  ELSE

    RETURN NEXT json_build_object('error', json_build_object('code', 404, 'message', format('Patch "%s" not found.', path)));

  END CASE;

  UPDATE http.log SET runtime = age(clock_timestamp(), cBegin) WHERE id = nId;

  RETURN;
EXCEPTION
WHEN others THEN
  GET STACKED DIAGNOSTICS vMessage = MESSAGE_TEXT, vContext = PG_EXCEPTION_CONTEXT;

  PERFORM http.write_to_log(path, headers, params, body, 'POST', vMessage, vContext);

  RETURN NEXT json_build_object('error', json_build_object('code', 500, 'message', vMessage));

  RETURN;
END
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- http.webhook ----------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.webhook (
  method    text,
  path      text,
  headers   jsonb,
  params    jsonb DEFAULT null,
  body      jsonb DEFAULT null
) RETURNS   json
AS $$
BEGIN
  RETURN json_build_object('code', 200, 'message', 'OK');
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP FETCH ------------------------------------------------------------------
--------------------------------------------------------------------------------
/**
 * Выполняет HTTP запрос.
 * @param {text} resource - Ресурс
 * @param {text} method - Метод
 * @param {jsonb} headers - HTTP заголовки
 * @param {text} content - Содержание запроса
 * @param {text} done - Имя функции обратного вызова в случае успешного ответа
 * @param {text} fail - Имя функции обратного вызова в случае сбоя
 * @param {text} agent - Агент
 * @param {text} profile - Профиль
 * @param {text} command - Команда
 * @param {text} message - Сообщение
 * @param {text} type - Способ отправки: native - родной; curl - через библиотеку cURL
 * @param {text} data - Произвольные данные в формате JSON
 * @return {uuid}
 */
CREATE OR REPLACE FUNCTION http.fetch (
  resource      text,
  method        text DEFAULT 'GET',
  headers       jsonb DEFAULT null,
  content       text DEFAULT null,
  done          text DEFAULT null,
  fail          text DEFAULT null,
  agent         text DEFAULT null,
  profile       text DEFAULT null,
  command       text DEFAULT null,
  message       text DEFAULT null,
  type          text DEFAULT null,
  data          jsonb DEFAULT null
) RETURNS       uuid
AS $$
BEGIN
  IF done IS NOT NULL THEN
    PERFORM FROM pg_namespace n INNER JOIN pg_proc p ON n.oid = p.pronamespace WHERE n.nspname = split_part(done, '.', 1) AND p.proname = split_part(done, '.', 2);
    IF NOT FOUND THEN
	  RAISE EXCEPTION 'Not found function: %', done;
    END IF;
  END IF;

  IF fail IS NOT NULL THEN
    PERFORM FROM pg_namespace n INNER JOIN pg_proc p ON n.oid = p.pronamespace WHERE n.nspname = split_part(fail, '.', 1) AND p.proname = split_part(fail, '.', 2);
    IF NOT FOUND THEN
	  RAISE EXCEPTION 'Not found function: %', fail;
    END IF;
  END IF;

  RETURN http.create_request(resource, type, method, headers, content, done, fail, agent, profile, command, message, data);
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- HTTP FETCH JSON -------------------------------------------------------------
--------------------------------------------------------------------------------
/**
 * Выполняет HTTP запрос.
 * @param {text} resource - Ресурс
 * @param {text} method - Метод
 * @param {jsonb} headers - HTTP заголовки
 * @param {json} content - Содержание запроса в формате JSON
 * @param {text} done - Имя функции обратного вызова в случае успешного ответа
 * @param {text} fail - Имя функции обратного вызова в случае сбоя
 * @param {text} agent - Агент
 * @param {text} profile - Профиль
 * @param {text} command - Команда
 * @param {text} message - Сообщение
 * @param {text} type - Способ отправки: native - родной; curl - через библиотеку cURL
 * @param {text} data - Произвольные данные в формате JSON
 * @return {uuid}
 */
CREATE OR REPLACE FUNCTION http.fetch (
  resource      text,
  method        text DEFAULT 'POST',
  headers       jsonb DEFAULT null,
  content       jsonb DEFAULT null,
  done          text DEFAULT null,
  fail          text DEFAULT null,
  agent         text DEFAULT null,
  profile       text DEFAULT null,
  command       text DEFAULT null,
  message       text DEFAULT null,
  type          text DEFAULT null,
  data          jsonb DEFAULT null
) RETURNS       uuid
AS $$
BEGIN
  IF headers IS NULL THEN
    headers := jsonb_build_object('Content-Type', 'application/json');
  END IF;

  RETURN http.create_request(resource, type, method, headers, content::text, done, fail, agent, profile, command, message, data);
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- http.done -------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.done (
  pRequest  uuid
) RETURNS   void
AS $$
BEGIN
  UPDATE http.request SET state = 2 WHERE id = pRequest;
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- http.fail -------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION http.fail (
  pRequest  uuid,
  pError    text
) RETURNS   void
AS $$
BEGIN
  UPDATE http.request SET state = 3, error = pError WHERE id = pRequest;
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;

--------------------------------------------------------------------------------
-- FUNCTION dec_to_hex ---------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION dec_to_hex (
  D          numeric,
  L          int default null
) RETURNS    text
AS
$$
DECLARE
  H          text;
  M          numeric;
  R          numeric;
BEGIN
  R := D;

  WHILE R > 0
  LOOP
    M := mod(R, 16);
    H := concat(
		   CASE
			 WHEN M < 10
			 THEN chr(CAST(M + 48 AS INTEGER))
			 ELSE chr(CAST(M + 87 AS INTEGER))
		   END, H);
    R := div(R, 16);
  END LOOP;

  IF H IS NULL THEN
    H := '0';
  END IF;

  IF L IS NOT NULL AND length(H) < L THEN
    RETURN lpad(H, L, '0');
  END IF;

  RETURN H;
END
$$ LANGUAGE plpgsql IMMUTABLE;

GRANT EXECUTE ON FUNCTION dec_to_hex(numeric, int) TO PUBLIC;

--------------------------------------------------------------------------------
-- URLEncode -------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION URLEncode (
  url       text
) RETURNS   text
AS $$
DECLARE
  result    text;
  c         text;
  i         int;
BEGIN
  result := '';

  FOR i IN 1..length(url)
  LOOP
    c := substr(url, i, 1);
    IF regexp_match(c, '[A-Za-z0-9_~.-]') IS NOT NULL THEN
      result := result || c;
    ELSE
      result := concat(result, '%', dec_to_hex(ascii(c), 2));
    END IF;
  END LOOP;

  RETURN result;
END
$$ LANGUAGE plpgsql IMMUTABLE;

GRANT EXECUTE ON FUNCTION URLEncode(text) TO PUBLIC;
