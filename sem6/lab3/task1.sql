CREATE OR REPLACE PROCEDURE compare_tables_topological (
    p_dev_schema  IN VARCHAR2,
    p_prod_schema IN VARCHAR2
) AS
    -- Standard nested table for the final list
    TYPE table_list_t IS TABLE OF VARCHAR2(128);
    v_sorted_tables table_list_t := table_list_t();
    
    -- Associative array (Hash Map) to track status and filter
    -- 0: unvisited, 1: visiting, 2: visited
    TYPE status_map_t IS TABLE OF NUMBER INDEX BY VARCHAR2(128); 
    v_status_map status_map_t;
    
    v_cycle_detected BOOLEAN := FALSE;

    -- Recursive DFS Procedure
    PROCEDURE visit(p_table_name VARCHAR2) AS
    BEGIN
        IF v_cycle_detected THEN RETURN; END IF;
        
        -- If currently visiting this table in the recursion stack, it's a cycle
        IF v_status_map.EXISTS(p_table_name) AND v_status_map(p_table_name) = 1 THEN
            v_cycle_detected := TRUE;
            DBMS_OUTPUT.PUT_LINE('ERROR: Circular dependency detected involving table: ' || p_table_name);
            RETURN;
        END IF;

        -- If table exists in our "target list" and hasn't been fully visited yet
        IF v_status_map.EXISTS(p_table_name) AND v_status_map(p_table_name) = 0 THEN
            v_status_map(p_table_name) := 1; -- Mark as 'Visiting'
            
            -- Find all parent tables (Foreign Keys)
            FOR r IN (
                SELECT DISTINCT r.table_name as ref_table
                FROM all_constraints c
                JOIN all_constraints r ON c.r_constraint_name = r.constraint_name AND c.r_owner = r.owner
                WHERE c.owner = UPPER(p_dev_schema)
                  AND c.table_name = UPPER(p_table_name)
                  AND c.constraint_type = 'R'
                  AND r.table_name <> p_table_name -- Ignore self-references
            ) LOOP
                -- Only visit the parent if it's also in our list of tables to process
                IF v_status_map.EXISTS(r.ref_table) THEN
                    visit(r.ref_table);
                END IF;
            END LOOP;
            
            v_status_map(p_table_name) := 2; -- Mark as 'Visited'
            v_sorted_tables.EXTEND;
            v_sorted_tables(v_sorted_tables.LAST) := p_table_name;
        END IF;
    END visit;

BEGIN
    -- 1. Identify tables and initialize v_status_map with 0 (unvisited)
    FOR r IN (
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)
        MINUS
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_prod_schema)
        UNION
        -- Tables with different structure
        SELECT dev.table_name
        FROM (SELECT table_name, column_name, data_type, data_length, nullable 
              FROM all_tab_columns WHERE owner = UPPER(p_dev_schema)) dev
        JOIN (SELECT table_name, column_name, data_type, data_length, nullable 
              FROM all_tab_columns WHERE owner = UPPER(p_prod_schema)) prod
        ON dev.table_name = prod.table_name AND dev.column_name = prod.column_name
        WHERE dev.data_type <> prod.data_type 
           OR dev.data_length <> prod.data_length 
           OR dev.nullable <> prod.nullable
    ) LOOP
        v_status_map(r.table_name) := 0;
    END LOOP;

    -- 2. Run DFS for each table in the map
    DECLARE
        v_tbl VARCHAR2(128);
    BEGIN
        v_tbl := v_status_map.FIRST;
        WHILE v_tbl IS NOT NULL LOOP
            IF v_status_map(v_tbl) = 0 THEN
                visit(v_tbl);
            END IF;
            v_tbl := v_status_map.NEXT(v_tbl);
        END LOOP;
    END;

    -- 3. Final Output
    IF NOT v_cycle_detected THEN
        DBMS_OUTPUT.PUT_LINE('Recommended creation order for ' || p_prod_schema || ':');
        IF v_sorted_tables.COUNT = 0 THEN
            DBMS_OUTPUT.PUT_LINE('No differences found.');
        ELSE
            FOR i IN 1..v_sorted_tables.COUNT LOOP
                DBMS_OUTPUT.PUT_LINE(i || '. ' || v_sorted_tables(i));
            END LOOP;
        END IF;
    END IF;
END;
