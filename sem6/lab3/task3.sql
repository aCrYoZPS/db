CREATE OR REPLACE PROCEDURE generate_sync_ddl (
    p_dev_schema  IN VARCHAR2,
    p_prod_schema IN VARCHAR2
) AUTHID CURRENT_USER AS
    TYPE object_list_t IS TABLE OF VARCHAR2(256);
    v_sorted_objects object_list_t := object_list_t();
    TYPE status_map_t IS TABLE OF NUMBER INDEX BY VARCHAR2(256);
    v_status_map status_map_t;
    v_cycle_detected BOOLEAN := FALSE;
    v_dev  VARCHAR2(128) := UPPER(TRIM(p_dev_schema));
    v_prod VARCHAR2(128) := UPPER(TRIM(p_prod_schema));

    PROCEDURE visit(p_obj_key VARCHAR2) AS
        v_type VARCHAR2(128); v_name VARCHAR2(128);
    BEGIN
        IF v_cycle_detected OR NOT v_status_map.EXISTS(p_obj_key) THEN RETURN; END IF;
        IF v_status_map(p_obj_key) = 1 THEN v_cycle_detected := TRUE; RETURN; END IF;
        IF v_status_map(p_obj_key) = 2 THEN RETURN; END IF;
        v_status_map(p_obj_key) := 1;
        v_type := SUBSTR(p_obj_key, 1, INSTR(p_obj_key, ':') - 1);
        v_name := SUBSTR(p_obj_key, INSTR(p_obj_key, ':') + 1);
        IF v_type = 'TABLE' THEN
            FOR r IN (SELECT DISTINCT r.table_name as ref_name FROM all_constraints c
                      JOIN all_constraints r ON c.r_constraint_name = r.constraint_name AND c.r_owner = r.owner
                      WHERE c.owner = v_dev AND c.table_name = v_name AND c.constraint_type = 'R'
                        AND r.table_name <> v_name)
            LOOP visit('TABLE:' || r.ref_name); END LOOP;
        ELSIF v_type = 'INDEX' THEN
            FOR r IN (SELECT table_name FROM all_indexes WHERE owner = v_dev AND index_name = v_name)
            LOOP visit('TABLE:' || r.table_name); END LOOP;
        ELSE
            FOR r IN (SELECT referenced_type, referenced_name FROM all_dependencies
                      WHERE owner = v_dev AND name = v_name AND type = v_type
                        AND referenced_owner = v_dev
                        AND (referenced_name <> v_name OR referenced_type <> v_type))
            LOOP visit(r.referenced_type || ':' || r.referenced_name); END LOOP;
        END IF;
        v_status_map(p_obj_key) := 2;
        v_sorted_objects.EXTEND; v_sorted_objects(v_sorted_objects.LAST) := p_obj_key;
    END visit;

    PROCEDURE print_ddl(p_type VARCHAR2, p_name VARCHAR2) AS
        v_ddl CLOB;
    BEGIN
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        v_ddl := DBMS_METADATA.GET_DDL(REPLACE(p_type, ' ', '_'), p_name, v_dev);
        v_ddl := REPLACE(v_ddl, '"' || v_dev || '"', '"' || v_prod || '"');
        DBMS_OUTPUT.PUT_LINE(v_ddl);
    EXCEPTION WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('-- Warning: Could not generate DDL for ' || p_type || ' ' || p_name || ': ' || SQLERRM);
    END;

BEGIN
    -- =========================================================
    -- FIX #3: Only new tables go into v_status_map (DROP+CREATE)
    -- =========================================================
    FOR r IN (
        SELECT table_name FROM all_tables WHERE owner = v_dev
        MINUS
        SELECT table_name FROM all_tables WHERE owner = v_prod
    ) LOOP
        v_status_map('TABLE:' || r.table_name) := 0;
    END LOOP;

    -- =========================================================
    -- FIX #1 + #2: Correct parentheses for source object detection
    -- =========================================================
    FOR r IN (
        (SELECT name, type FROM all_source WHERE owner = v_dev GROUP BY name, type
         MINUS
         SELECT name, type FROM all_source WHERE owner = v_prod GROUP BY name, type)
        UNION
        (SELECT DISTINCT name, type FROM all_source WHERE owner = v_dev
         INTERSECT
         SELECT name, type FROM (
             ((SELECT name, type, line, RTRIM(text, CHR(13)||CHR(10)||' ') as txt
               FROM all_source WHERE owner = v_dev)
              MINUS
              (SELECT name, type, line, RTRIM(text, CHR(13)||CHR(10)||' ') as txt
               FROM all_source WHERE owner = v_prod))
             UNION
             ((SELECT name, type, line, RTRIM(text, CHR(13)||CHR(10)||' ') as txt
               FROM all_source WHERE owner = v_prod)
              MINUS
              (SELECT name, type, line, RTRIM(text, CHR(13)||CHR(10)||' ') as txt
               FROM all_source WHERE owner = v_dev))
         ))
    ) LOOP
        v_status_map(r.type || ':' || r.name) := 0;
    END LOOP;

    -- =========================================================
    -- FIX #1: Correct parentheses for index detection
    -- =========================================================
    FOR r IN (
        (SELECT index_name FROM all_indexes i
         WHERE owner = v_dev
           AND NOT EXISTS (SELECT 1 FROM all_constraints c
                           WHERE c.owner = i.owner AND c.index_name = i.index_name
                             AND c.constraint_type IN ('P','U'))
         MINUS
         SELECT index_name FROM all_indexes WHERE owner = v_prod)
        UNION
        (SELECT index_name FROM all_indexes WHERE owner = v_dev
         INTERSECT
         SELECT index_name FROM (
             ((SELECT index_name, column_name, column_position
               FROM all_ind_columns WHERE index_owner = v_dev)
              MINUS
              (SELECT index_name, column_name, column_position
               FROM all_ind_columns WHERE index_owner = v_prod))
             UNION
             ((SELECT index_name, column_name, column_position
               FROM all_ind_columns WHERE index_owner = v_prod)
              MINUS
              (SELECT index_name, column_name, column_position
               FROM all_ind_columns WHERE index_owner = v_dev))
         ))
    ) LOOP
        v_status_map('INDEX:' || r.index_name) := 0;
    END LOOP;

    DECLARE v_key VARCHAR2(256) := v_status_map.FIRST;
    BEGIN
        WHILE v_key IS NOT NULL LOOP
            IF v_status_map(v_key) = 0 THEN visit(v_key); END IF;
            v_key := v_status_map.NEXT(v_key);
        END LOOP;
    END;

    IF v_cycle_detected THEN DBMS_OUTPUT.PUT_LINE('-- Error: Cycle detected.'); RETURN; END IF;

    DBMS_OUTPUT.PUT_LINE('-- SYNC SCRIPT FOR ' || v_prod);

    -- Drop objects that exist in prod but not in dev
    FOR r IN (SELECT table_name FROM all_tables WHERE owner = v_prod
              MINUS SELECT table_name FROM all_tables WHERE owner = v_dev) LOOP
        DBMS_OUTPUT.PUT_LINE('DROP TABLE "' || v_prod || '"."' || r.table_name || '" CASCADE CONSTRAINTS;');
    END LOOP;
    FOR r IN (SELECT name, type FROM all_source WHERE owner = v_prod GROUP BY name, type
              MINUS SELECT name, type FROM all_source WHERE owner = v_dev GROUP BY name, type) LOOP
        DBMS_OUTPUT.PUT_LINE('DROP ' || r.type || ' "' || v_prod || '"."' || r.name || '";');
    END LOOP;
    FOR r IN (SELECT index_name FROM all_indexes i WHERE owner = v_prod
              AND NOT EXISTS (SELECT 1 FROM all_constraints c
                              WHERE c.owner = i.owner AND c.index_name = i.index_name
                                AND c.constraint_type IN ('P','U'))
              MINUS SELECT index_name FROM all_indexes WHERE owner = v_dev) LOOP
        DBMS_OUTPUT.PUT_LINE('DROP INDEX "' || v_prod || '"."' || r.index_name || '";');
    END LOOP;

    -- Drop objects being replaced (reverse topological order)
    FOR i IN REVERSE 1..v_sorted_objects.COUNT LOOP
        DECLARE
            v_type   VARCHAR2(128) := SUBSTR(v_sorted_objects(i), 1, INSTR(v_sorted_objects(i),':')-1);
            v_name   VARCHAR2(128) := SUBSTR(v_sorted_objects(i), INSTR(v_sorted_objects(i),':')+1);
            v_exists NUMBER;
        BEGIN
            IF v_type = 'TABLE' THEN
                SELECT COUNT(*) INTO v_exists FROM all_tables WHERE owner = v_prod AND table_name = v_name;
                IF v_exists > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('DROP TABLE "' || v_prod || '"."' || v_name || '" CASCADE CONSTRAINTS;');
                END IF;
            ELSIF v_type = 'INDEX' THEN
                SELECT COUNT(*) INTO v_exists FROM all_indexes WHERE owner = v_prod AND index_name = v_name;
                IF v_exists > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('DROP INDEX "' || v_prod || '"."' || v_name || '";');
                END IF;
            ELSE
                SELECT COUNT(*) INTO v_exists FROM all_objects
                WHERE owner = v_prod AND object_name = v_name AND object_type = v_type;
                IF v_exists > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('DROP ' || v_type || ' "' || v_prod || '"."' || v_name || '";');
                END IF;
            END IF;
        END;
    END LOOP;

    -- =========================================================
    -- FIX #3: ALTER TABLE for column differences in existing tables
    -- =========================================================
    FOR r IN (
        SELECT table_name FROM all_tables WHERE owner = v_dev
        INTERSECT SELECT table_name FROM all_tables WHERE owner = v_prod
        INTERSECT
        SELECT table_name FROM (
            ((SELECT table_name, column_name, data_type, data_length,
                     data_precision, data_scale, char_length, nullable
              FROM all_tab_columns WHERE owner = v_dev)
             MINUS
             (SELECT table_name, column_name, data_type, data_length,
                     data_precision, data_scale, char_length, nullable
              FROM all_tab_columns WHERE owner = v_prod))
            UNION
            ((SELECT table_name, column_name, data_type, data_length,
                     data_precision, data_scale, char_length, nullable
              FROM all_tab_columns WHERE owner = v_prod)
             MINUS
             (SELECT table_name, column_name, data_type, data_length,
                     data_precision, data_scale, char_length, nullable
              FROM all_tab_columns WHERE owner = v_dev))
        )
    ) LOOP
        -- DROP columns present in prod but removed in dev
        FOR col IN (
            SELECT column_name FROM all_tab_columns WHERE owner = v_prod AND table_name = r.table_name
            MINUS
            SELECT column_name FROM all_tab_columns WHERE owner = v_dev  AND table_name = r.table_name
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                'ALTER TABLE "' || v_prod || '"."' || r.table_name ||
                '" DROP COLUMN "' || col.column_name || '";');
        END LOOP;

        -- ADD columns present in dev but missing from prod
        FOR col IN (
            SELECT d.column_name, d.data_type, d.data_precision, d.data_scale,
                   d.char_length, d.nullable, d.data_default
            FROM   all_tab_columns d
            WHERE  d.owner = v_dev AND d.table_name = r.table_name
              AND  d.column_name NOT IN (
                       SELECT column_name FROM all_tab_columns
                       WHERE  owner = v_prod AND table_name = r.table_name)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                'ALTER TABLE "' || v_prod || '"."' || r.table_name ||
                '" ADD "' || col.column_name || '" ' || col.data_type ||
                CASE col.data_type
                    WHEN 'VARCHAR2'  THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'NVARCHAR2' THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'CHAR'      THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'NCHAR'     THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'NUMBER'    THEN
                        CASE WHEN col.data_precision IS NOT NULL
                             THEN '(' || col.data_precision ||
                                  CASE WHEN col.data_scale IS NOT NULL
                                       THEN ',' || col.data_scale ELSE '' END || ')'
                             ELSE '' END
                    ELSE ''
                END ||
                CASE WHEN col.nullable    = 'N'   THEN ' NOT NULL' ELSE '' END ||
                CASE WHEN col.data_default IS NOT NULL THEN ' DEFAULT ' || col.data_default ELSE '' END ||
                ';');
        END LOOP;

        -- MODIFY columns that exist in both but whose type/attributes changed
        FOR col IN (
            SELECT d.column_name, d.data_type, d.data_precision, d.data_scale,
                   d.char_length, d.nullable, d.data_default
            FROM   all_tab_columns d
            JOIN   all_tab_columns p
                   ON  p.owner = v_prod AND p.table_name = d.table_name
                   AND p.column_name = d.column_name
            WHERE  d.owner = v_dev AND d.table_name = r.table_name
              AND (d.data_type       <> p.data_type
                OR d.char_length     <> p.char_length
                OR NVL(d.data_precision,-1) <> NVL(p.data_precision,-1)
                OR NVL(d.data_scale,   -1) <> NVL(p.data_scale,   -1)
                OR d.nullable        <> p.nullable)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                'ALTER TABLE "' || v_prod || '"."' || r.table_name ||
                '" MODIFY "' || col.column_name || '" ' || col.data_type ||
                CASE col.data_type
                    WHEN 'VARCHAR2'  THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'NVARCHAR2' THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'CHAR'      THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'NCHAR'     THEN '(' || col.char_length  || ' CHAR)'
                    WHEN 'NUMBER'    THEN
                        CASE WHEN col.data_precision IS NOT NULL
                             THEN '(' || col.data_precision ||
                                  CASE WHEN col.data_scale IS NOT NULL
                                       THEN ',' || col.data_scale ELSE '' END || ')'
                             ELSE '' END
                    ELSE ''
                END ||
                CASE WHEN col.nullable = 'N' THEN ' NOT NULL' ELSE ' NULL' END ||
                CASE WHEN col.data_default IS NOT NULL THEN ' DEFAULT ' || col.data_default ELSE '' END ||
                ';');
        END LOOP;
    END LOOP;

    -- Create/replace objects (topological order)
    FOR i IN 1..v_sorted_objects.COUNT LOOP
        DECLARE
            v_type VARCHAR2(128) := SUBSTR(v_sorted_objects(i), 1, INSTR(v_sorted_objects(i),':')-1);
            v_name VARCHAR2(128) := SUBSTR(v_sorted_objects(i), INSTR(v_sorted_objects(i),':')+1);
        BEGIN
            print_ddl(v_type, v_name);
        END;
    END LOOP;
END;