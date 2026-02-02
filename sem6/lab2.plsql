-- 1
create table students
(
    id       number,
    name     varchar2(255),
    group_id number
);
create table groups
(
    id    number,
    name  varchar2(255),
    c_val number
);

-- 2 + 6
create or replace trigger students_before_insert
    before insert
    on STUDENTS
    for each row
declare
    v_max_id          number;
    v_duplicate_count number := 0;
    v_old_c_val       number := 0;
begin
    IF RECOVERY_CTX.IS_RECOVERING THEN
        RETURN;
    end if;

    select count(*)
    into v_duplicate_count
    from (SELECT ID, COUNT(ID) as cnt
          FROM STUDENTS
          GROUP BY ID
          HAVING COUNT(ID) > 1);

    if v_duplicate_count > 0 then
        raise_application_error(-20000, 'The table is malformed, please remove non-unique id');
    end if;

    begin
        select C_VAL into v_old_c_val from GROUPS where ID = :NEW.GROUP_ID;
    exception
        when NO_DATA_FOUND then
            raise_application_error(-20002, 'The group with id ' || :NEW.GROUP_ID || ' does not exist');
    end;

    update GROUPS set C_VAL = v_old_c_val + 1 where ID = :NEW.GROUP_ID;

    select nvl(max(id), 0)
    into v_max_id
    from STUDENTS;
    :NEW.ID := v_max_id + 1;
end;

create or replace trigger groups_before_insert
    before insert
    on GROUPS
    for each row
declare
    v_max_id          number;
    v_duplicate_count number := 0;
begin
    IF RECOVERY_CTX.IS_RECOVERING THEN
        RETURN;
    end if;

    select count(*)
    into v_duplicate_count
    from (SELECT ID, COUNT(ID) as cnt
          FROM GROUPS
          GROUP BY ID
          HAVING COUNT(ID) > 1);

    if v_duplicate_count > 0 then
        raise_application_error(-20000, 'The table is malformed, please remove non-unique id');
    end if;

    select count(*)
    into v_duplicate_count
    from GROUPS
    WHERE NAME = :NEW.NAME;

    if v_duplicate_count > 0 then
        raise_application_error(-20001, 'Group with name ' || :NEW.NAME || ' already exists');
    end if;

    select nvl(max(id), 0) into v_max_id from GROUPS;
    :NEW.ID := v_max_id + 1;
    :NEW.C_VAL := 0;
end;

insert into STUDENTS
values (1, 's1', 2);
commit;


create or replace trigger students_before_update
    before update
    on STUDENTS
    for each row
declare
    v_old_c_val number;
    v_new_c_val number;
begin
    IF RECOVERY_CTX.IS_RECOVERING THEN
        RETURN;
    end if;

    if :NEW.ID != :OLD.ID then
        raise_application_error(-20004, 'Cannot change id of a student');
    end if;

    declare
        v_test number;
    begin
        select 1 into v_test from GROUPS where ID = :NEW.GROUP_ID;
    exception
        when NO_DATA_FOUND then
            raise_application_error(-20002, 'The group with id ' || :NEW.GROUP_ID || ' does not exist');
    end;

    select C_VAL into v_old_c_val from GROUPS where ID = :OLD.GROUP_ID;
    select C_VAL into v_new_c_val from GROUPS where ID = :NEW.GROUP_ID;

    update GROUPS set C_VAL=v_old_c_val - 1 where ID = :OLD.GROUP_ID;
    update GROUPS set C_VAL=v_new_c_val + 1 where ID = :NEW.GROUP_ID;
end;

--create or replace trigger groups_before_update
--    before update
--    on GROUPS
--    for each row
--declare
--    v_group_w_name_count number;
--begin
--    RETURN;
--    IF RECOVERY_CTX.IS_RECOVERING THEN
--        RETURN;
--    end if;
--
--    if :NEW.ID != :OLD.ID then
--        raise_application_error(-20005, 'Cannot change id of a group');
--    end if;
--
--    if :NEW.C_VAL != :OLD.C_VAL then
--        raise_application_error(-20006, 'Cannot change student count of a group');
--    end if;
--
--    select count(*) into v_group_w_name_count from GROUPS where NAME = :NEW.NAME;
--    if v_group_w_name_count > 0 then
--        raise_application_error(-20001, 'Group with name ' || :NEW.NAME || ' already exists');
--    end if;
--end;

-- 3
create or replace trigger groups_before_delete_cascade
    before delete
    on GROUPS
    for each row
begin
    IF RECOVERY_CTX.IS_RECOVERING THEN
        RETURN;
    end if;
    delete from STUDENTS where GROUP_ID = :OLD.ID;
end;

create or replace trigger students_after_delete_cascade
    before delete
    on STUDENTS
    for each row
declare
    v_old_count number;
begin
    IF RECOVERY_CTX.IS_RECOVERING THEN
        RETURN;
    end if;
    select C_VAL
    into
        v_old_count
    from GROUPS
    where ID = :OLD.GROUP_ID;
    update GROUPS set C_VAL = v_old_count - 1 where ID = :OLD.GROUP_ID;
exception
    when NO_DATA_FOUND then
        DBMS_OUTPUT.PUT_LINE('fuck');
end;

-- 4
CREATE OR REPLACE TRIGGER log_dml_changes
    AFTER INSERT OR UPDATE OR DELETE
    ON STUDENTS
    FOR EACH ROW
DECLARE
    v_old_json varchar2(1024);
    v_new_json varchar2(1024);
BEGIN
    IF RECOVERY_CTX.IS_RECOVERING THEN
        RETURN;
    end if;
    IF INSERTING THEN
        v_new_json := JSON_OBJECT(
                'id' VALUE :NEW.ID,
                'name' VALUE :NEW.NAME,
                'group_id' VALUE :NEW.GROUP_ID
                RETURNING VARCHAR2);

        INSERT INTO AUDIT_LOG (table_name, operation, log_time, new_data)
        VALUES ('STUDENTS', 'INSERT', SYSTIMESTAMP, v_new_json); -- Adapt for columns
    ELSIF UPDATING THEN
        v_old_json := JSON_OBJECT(
                'id' VALUE :OLD.ID,
                'name' VALUE :OLD.NAME,
                'group_id' VALUE :OLD.GROUP_ID
                RETURNING VARCHAR2);
        v_new_json := JSON_OBJECT(
                'id' VALUE :NEW.ID,
                'name' VALUE :NEW.NAME,
                'group_id' VALUE :NEW.GROUP_ID
                RETURNING VARCHAR2);
        INSERT INTO AUDIT_LOG (table_name, operation, log_time, old_data, new_data)
        VALUES ('STUDENTS', 'UPDATE', SYSTIMESTAMP, v_old_json, v_new_json);
    ELSIF DELETING THEN
        v_old_json := JSON_OBJECT(
                'id' VALUE :OLD.ID,
                'name' VALUE :OLD.NAME,
                'group_id' VALUE :OLD.GROUP_ID
                RETURNING VARCHAR2);
        INSERT INTO AUDIT_LOG(table_name, operation, log_time, old_data)
        VALUES ('STUDENTS', 'DELETE', SYSTIMESTAMP, v_old_json);
    END IF;
END;

-- 5
CREATE OR REPLACE PACKAGE RECOVERY_CTX IS
    IS_RECOVERING BOOLEAN := FALSE;
END;

CREATE OR REPLACE PACKAGE DATA_RECOVERY AS
    PROCEDURE REVERT_ROW_FROM_JSON(
        p_table_name IN VARCHAR2,
        p_operation IN VARCHAR2,
        p_json_data IN VARCHAR2,
        p_pk_col IN VARCHAR2 DEFAULT 'ID'
    );

    PROCEDURE RESTORE_TO_TIMESTAMP(
        p_target_table IN VARCHAR2,
        p_target_time IN TIMESTAMP
    );

    PROCEDURE RESTORE_WITH_OFFSET(
        p_target_table IN VARCHAR2,
        p_offset IN INTERVAL DAY TO SECOND
    );

    PROCEDURE RECALCULATE_STUDENT_COUNT;
END DATA_RECOVERY;

CREATE OR REPLACE PACKAGE BODY DATA_RECOVERY AS
    PROCEDURE REVERT_ROW_FROM_JSON(
        p_table_name IN VARCHAR2,
        p_operation IN VARCHAR2,
        p_json_data IN VARCHAR2,
        p_pk_col IN VARCHAR2 DEFAULT 'ID'
    ) IS
        v_jo        JSON_OBJECT_T;
        v_keys      JSON_KEY_LIST;
        v_key       VARCHAR2(128);
        v_sql       CLOB;
        v_cols_part CLOB;
        v_vals_part CLOB;
        v_set_part  CLOB;
        v_pk_val    VARCHAR2(512);
    BEGIN
        v_jo := JSON_OBJECT_T.parse(p_json_data);
        v_keys := v_jo.get_Keys();
        v_pk_val := json_value(p_json_data, '$.' || lower(p_pk_col));

        IF p_operation = 'DELETE' THEN
            v_sql := 'INSERT INTO ' || p_table_name || ' (';

            FOR i IN 1 .. v_keys.COUNT
                LOOP
                    v_key := v_keys(i);
                    IF i > 1 THEN
                        v_cols_part := v_cols_part || ', ';
                        v_vals_part := v_vals_part || ', ';
                    END IF;

                    v_cols_part := v_cols_part || v_key;

                    v_vals_part := v_vals_part || 'JSON_VALUE(:j, ''$.' || v_key || ''')';
                END LOOP;

            v_sql := v_sql || v_cols_part || ') VALUES (' || v_vals_part || ')';

            EXECUTE IMMEDIATE v_sql USING p_json_data,p_json_data,p_json_data;

        ELSIF p_operation = 'UPDATE' THEN
            v_sql := 'UPDATE ' || p_table_name || ' SET ';

            FOR i IN 1 .. v_keys.COUNT
                LOOP
                    v_key := v_keys(i);

                    IF UPPER(v_key) != UPPER(p_pk_col) THEN
                        IF LENGTH(v_set_part) > 0 THEN
                            v_set_part := v_set_part || ', ';
                        END IF;

                        v_set_part := v_set_part || v_key || ' = JSON_VALUE(:j, ''$.' || v_key || ''')';
                    END IF;
                END LOOP;

            v_sql := v_sql || v_set_part || ' WHERE ' || p_pk_col || ' = :pk';

            EXECUTE IMMEDIATE v_sql USING p_json_data,p_json_data, v_pk_val;

        ELSIF p_operation = 'INSERT' THEN
            v_sql := 'DELETE FROM ' || p_table_name || ' WHERE ' || p_pk_col || ' = :pk';
            EXECUTE IMMEDIATE v_sql USING v_pk_val;
        END IF;

        DBMS_OUTPUT.PUT_LINE('Reverted ' || p_operation || ' using JSON.' || v_sql);

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error reverting JSON: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);
            RAISE;
    END;

    PROCEDURE RESTORE_TO_TIMESTAMP(
        p_target_table IN VARCHAR2,
        p_target_time IN TIMESTAMP
    ) IS
    BEGIN
        RECOVERY_CTX.IS_RECOVERING := true;

        FOR r IN (
            SELECT *
            FROM AUDIT_LOG
            WHERE TABLE_NAME = p_target_table
              AND LOG_TIME > p_target_time
            ORDER BY ID DESC
            )
            LOOP
                REVERT_ROW_FROM_JSON(
                        p_table_name => r.TABLE_NAME,
                        p_operation => r.OPERATION,
                        p_json_data => CASE WHEN r.OPERATION = 'INSERT' THEN r.NEW_DATA ELSE r.OLD_DATA END
                );
            END LOOP;

        IF p_target_table = 'STUDENTS' THEN
            RECALCULATE_STUDENT_COUNT();
        end if;

        COMMIT;
        RECOVERY_CTX.IS_RECOVERING := false;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RECOVERY_CTX.IS_RECOVERING := false;
            RAISE;
    END;

    PROCEDURE RESTORE_WITH_OFFSET(
        p_target_table IN VARCHAR2,
        p_offset IN INTERVAL DAY TO SECOND
    ) IS
    BEGIN
        RESTORE_TO_TIMESTAMP(p_target_table, SYSTIMESTAMP - p_offset);
    END;


    PROCEDURE RECALCULATE_STUDENT_COUNT IS
    begin
        UPDATE GROUPS
        SET C_VAL = (SELECT COUNT(*)
                     FROM STUDENTS
                     WHERE GROUP_ID = GROUPS.ID)
        WHERE EXISTS (SELECT 1
                      FROM STUDENTS
                      WHERE GROUP_ID = GROUPS.ID);
    end;
END DATA_RECOVERY;

BEGIN
    DATA_RECOVERY.RESTORE_TO_TIMESTAMP(
            p_target_table => 'STUDENTS',
            p_target_time => TO_TIMESTAMP('2026-01-31 18:43:00', 'YYYY-MM-DD HH24:MI:SS')
    );
END;
