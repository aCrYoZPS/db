-- 1
create table MyTable(
    id number,
    val number
)

-- 2 
DECLARE
    counter int;
    id      pls_integer;
    val     pls_integer;
BEGIN
    FOR counter IN 1..10000
        LOOP
            id := DBMS_RANDOM.RANDOM();
            val := DBMS_RANDOM.RANDOM();
            INSERT INTO MYTABLE (id, val) VALUES (id, val);
        END LOOP;
    COMMIT;
END;

-- 3 
create or replace function even_odd return varchar2 is
    result varchar(10);
    even   pls_integer;
    odd    pls_integer;
begin
    select count(*) into even from MYTABLE where MOD(abs(VAL), 2) = 0;
    select count(*) into odd from MYTABLE where MOD(abs(VAL), 2) = 1;
    DBMS_OUTPUT.PUT_LINE('odd: ' || odd);
    DBMS_OUTPUT.PUT_LINE('even: ' || even);
    if even > odd then
        result := 'TRUE';
    elsif odd > even then
        result := 'FALSE';
    else
        result := 'EQUAL';
    end if;

    return result;
end;
begin
    DBMS_OUTPUT.PUT_LINE(even_odd());
end;

--4
create or replace function gen_insert(p_id in int) return varchar2 is
    result varchar2(255);
    v_val  number;
begin
    select val into v_val from MYTABLE where ID = p_id;
    result := 'insert into MYTABLE values (' || p_id || ',' || v_val || ');';
    return result;
end;

begin
    DBMS_OUTPUT.PUT_LINE(gen_insert(805789915));
end;

--5
create or replace procedure insert_into_mytable(p_id number, p_val number) is
begin
    insert into MYTABLE values (p_id, p_val);
    commit;
    DBMS_OUTPUT.PUT_LINE('Inserted row with id ' || p_id || ', value: ' || p_val);
end;

create or replace procedure update_mytable(p_id number, p_val number) is
begin
    update MYTABLE set VAL = p_val where ID = p_id;
    commit;
    DBMS_OUTPUT.PUT_LINE('Updated row with id ' || p_id || ' new value: ' || p_val);
end;

create or replace procedure delete_from_mytable(p_id number) is
begin
    delete from MYTABLE where ID = p_id;
    commit;
    DBMS_OUTPUT.PUT_LINE('Deleted row with id ' || p_id);
end;

begin
    insert_into_mytable(11, 1);
    update_mytable(11, 42);
    delete_from_mytable(11);
end;

--6
create or replace function calculate_annual_reward(p_monthly_salary in number, p_bonus_percent in int) return number is
    result number;
begin
    if p_monthly_salary is null then
        raise_application_error(-20000, 'Monthly salary cannot be NULL');
    elsif p_monthly_salary < 0 then
        raise_application_error(-20001, 'Monthly salary cannot be <0');
    end if;

    if p_bonus_percent is null then
        raise_application_error(-20002, 'Bonus percent cannot be NULL');
    elsif p_bonus_percent < 0 then
        raise_application_error(-20003, 'Bonus percent cannot be <0');
    end if;

    result := (1 + p_bonus_percent / 100) * 12 * p_monthly_salary;

    return result;
end;

SELECT calculate_annual_reward(100000, 10) AS annual_reward
FROM dual;
SELECT calculate_annual_reward(-1000, 10)
FROM dual;
SELECT calculate_annual_reward(100000, -5)
FROM dual;
SELECT calculate_annual_reward(NULL, 10)
FROM dual;
SELECT calculate_annual_reward('NULL', 10)
FROM dual;
SELECT calculate_annual_reward(10000, '10')
FROM dual;

