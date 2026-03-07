-- PROD CONSOLE
CREATE TABLE tbl1
(
    id    NUMBER PRIMARY KEY,
    value VARCHAR2(50)
);

create or replace procedure proc1(msg in VARCHAR2)
as
begin
    DBMS_OUTPUT.PUT_LINE('proc1:' || msg);
end;

CREATE OR REPLACE PACKAGE useless_flags AS
    f_debug_enabled BOOLEAN := TRUE;
    f_process_active BOOLEAN := FALSE;
    f_max_retries NUMBER := 3;
END useless_flags;

-- DEV CONSOLE
CREATE TABLE tbl1
(
    id    NUMBER PRIMARY KEY,
    value VARCHAR2(50)
);

CREATE TABLE tbl2
(
    id      NUMBER PRIMARY KEY,
    tbl1_id NUMBER REFERENCES tbl1 (id),
    tbl3_id Number REFERENCES tbl3 (id),
    value   VARCHAR2(50)
);

CREATE TABLE tbl3
(
    id      NUMBER PRIMARY KEY,
    tbl1_id NUMBER REFERENCES tbl1 (id),
    value   VARCHAR2(50),
    diff    NUMBER NULL
);


create or replace procedure proc1(msg in VARCHAR2)
as
begin
    DBMS_OUTPUT.PUT_LINE('proc1:' || msg);
    DBMS_OUTPUT.PUT_LINE('proc1-2:' || msg);
end;

create or replace procedure proc2(msg in VARCHAR2)
as
    v_id NUMBER;
begin
    DBMS_OUTPUT.PUT_LINE('proc2:' || msg);
    proc1(msg);
    select id
    into v_id
    from tbl3
    where value = 'ABC';
end;

CREATE OR REPLACE PACKAGE my_flags AS
    f_debug_enabled BOOLEAN := TRUE;
    f_process_active BOOLEAN := FALSE;
    f_max_retries NUMBER := 3;
END my_flags;

begin
    proc2('aaa');
end;
