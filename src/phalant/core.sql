CREATE TYPE phalant AS (
  id text[],
  pointer text[],
  value text
);


CREATE OR REPLACE FUNCTION phalant_delete(t text, path text[]) RETURNS void AS $$
DECLARE id_sub text;
BEGIN
    SET id_sub = format('[1:%s]', array_length(path));
    EXECUTE format('DELETE FROM %s WHERE id%s = %L;', t, id_sub, path);
    EXECUTE format('UPDATE %s SET pointer = array_remove(pointer, %L) WHERE id = %L;',
                   t, path[array_length(path, 1)], path[1:array_length(path, 1) - 1]);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION jsonb_array_starts_with(target jsonb, beginning jsonb) RETURNS boolean AS $$
DECLARE
  target_length int;
  beginning_length int;
BEGIN
  SELECT jsonb_array_length(target) INTO target_length;
  SELECT jsonb_array_length(beginning) INTO beginning_length;
  IF beginning_length > target_length THEN
    RETURN false;
  ELSE
    FOR i IN 0..beginning_length LOOP
      IF target->>i != beginning->>i THEN
        RETURN false;
      END IF;
    END LOOP;
  END IF;
  RETURN true;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION phalant_drop(t text, path text[]) RETURNS void AS $$
DECLARE current text;
BEGIN
EXECUTE format('SELECT value FROM %s WHERE id = %L', t, path) INTO current;
  IF current = '[]' THEN
    EXECUTE format('DELETE
                     FROM %s
                     WHERE id =
                     (SELECT id
                      FROM %s
                      WHERE cardinality(id) = cardinality(%L::text[]) + 1
                        AND array_starts_with(id, %L)
                      ORDER BY id DESC
                      LIMIT 1)',
                    t, t, path, path, path);
  ELSE
    EXECUTE format('DELETE
                     FROM %s
                     WHERE id =
                     (SELECT id
                      FROM %s
                      WHERE cardinality(id) = cardinality(%L::text[]) + 1
                        AND array_starts_with(id, %L)
                      LIMIT 1)',
                    t, t, path, path, path);
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION phalant_insert(t text, path text[], value text) RETURNS void AS $$
DECLARE current phalant;
BEGIN
  EXECUTE format('SELECT value FROM %s WHERE id = %L') INTO current;
  CASE current
    WHEN '{}' THEN

    WHEN '#{}' THEN
    WHEN '[]' THEN
