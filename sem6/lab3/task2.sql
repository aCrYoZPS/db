CREATE OR REPLACE PROCEDURE compare_all_objects (
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
            FOR r IN (SELECT DISTINCT r.table_name as ref_name FROM all_constraints c JOIN all_constraints r ON c.r_constraint_name = r.constraint_name AND c.r_owner = r.owner WHERE c.owner = v_dev AND c.table_name = v_name AND c.constraint_type = 'R' AND r.table_name <> v_name) LOOP visit('TABLE:' || r.ref_name); END LOOP;
        ELSIF v_type = 'INDEX' THEN
            FOR r IN (SELECT table_name FROM all_indexes WHERE owner = v_dev AND index_name = v_name) LOOP visit('TABLE:' || r.table_name); END LOOP;
        ELSE
            FOR r IN (SELECT referenced_type, referenced_name FROM all_dependencies WHERE owner = v_dev AND name = v_name AND type = v_type AND referenced_owner = v_dev AND (referenced_name <> v_name OR referenced_type <> v_type)) LOOP visit(r.referenced_type || ':' || r.referenced_name); END LOOP;
        END IF;
        v_status_map(p_obj_key) := 2;
        v_sorted_objects.EXTEND; v_sorted_objects(v_sorted_objects.LAST) := p_obj_key;
    END visit;
BEGIN
    FOR r IN (SELECT table_name FROM all_tables WHERE owner = v_dev MINUS SELECT table_name FROM all_tables WHERE owner = v_prod UNION SELECT dev.table_name FROM (SELECT table_name, column_name, data_type, data_length FROM all_tab_columns WHERE owner = v_dev) dev JOIN (SELECT table_name, column_name, data_type, data_length FROM all_tab_columns WHERE owner = v_prod) prod ON dev.table_name = prod.table_name AND dev.column_name = prod.column_name WHERE dev.data_type <> prod.data_type OR dev.data_length <> prod.data_length) LOOP v_status_map('TABLE:' || r.table_name) := 0; END LOOP;
    FOR r IN (SELECT name, type FROM all_source WHERE owner = v_dev GROUP BY name, type MINUS SELECT name, type FROM all_source WHERE owner = v_prod GROUP BY name, type UNION SELECT name, type FROM (SELECT name, type, line, text FROM all_source WHERE owner = v_dev MINUS SELECT name, type, line, text FROM all_source WHERE owner = v_prod)) LOOP v_status_map(r.type || ':' || r.name) := 0; END LOOP;
    FOR r IN (SELECT index_name FROM all_indexes WHERE owner = v_dev MINUS SELECT index_name FROM all_indexes WHERE owner = v_prod) LOOP v_status_map('INDEX:' || r.index_name) := 0; END LOOP;
    DECLARE v_key VARCHAR2(256) := v_status_map.FIRST; BEGIN WHILE v_key IS NOT NULL LOOP IF v_status_map(v_key) = 0 THEN visit(v_key); END IF; v_key := v_status_map.NEXT(v_key); END LOOP; END;
    IF NOT v_cycle_detected THEN
        DBMS_OUTPUT.PUT_LINE('--- SYNC PLAN FOR ' || v_prod || ' ---');
        IF v_sorted_objects.COUNT = 0 THEN DBMS_OUTPUT.PUT_LINE('Schemas are identical.');
        ELSE FOR i IN 1..v_sorted_objects.COUNT LOOP DBMS_OUTPUT.PUT_LINE(i || '. ' || v_sorted_objects(i)); END LOOP; END IF;
    END IF;
END;
/