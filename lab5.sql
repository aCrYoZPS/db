# CREATE PROCEDURE update_device_stats(IN device_id_param CHAR(36), IN file_size INT)
# BEGIN
#     UPDATE device_stats
#     set total_file_count=total_file_count + 1,
#         total_sent_bytes=total_sent_bytes + file_size,
#         last_upload_time=NOW(),
#         max_file_size=GREATEST(max_file_size, file_size),
#         active_since=IFNULL(active_since, NOW())
#     where device_id = device_id_param;
# end;


CREATE TRIGGER trg_after_insert_document
    AFTER INSERT
    ON documents_device
    FOR EACH ROW
BEGIN
    UPDATE device_stats
    set total_file_count=total_file_count + 1,
        total_sent_bytes=total_sent_bytes + new.file_size,
        last_upload_time=NOW(),
        max_file_size=GREATEST(max_file_size, new.file_size),
        active_since=IFNULL(active_since, NOW())
    where device_id = new.device_id;
end;

CREATE PROCEDURE create_customer_contact(IN contact_id CHAR(36), IN customer_id_param CHAR(36),
                                         IN email_param VARCHAR(255),
                                         IN notifications_param VARCHAR(255))
BEGIN
    INSERT INTO customer_contact_data VALUES (contact_id, customer_id_param, email_param, notifications_param);
end;

CREATE PROCEDURE create_customer(IN new_customer_id CHAR(36), IN customer_name VARCHAR(255),
                                 IN customer_number VARCHAR(255),
                                 in country_id_param CHAR(36), IN email_param VARCHAR(255),
                                 IN notifications_param VARCHAR(255))
begin
    insert into customers
    values (new_customer_id, customer_name, customer_number, country_id_param, 'PROCEDURE', NOW());
    insert into customer_contact_data values (UUID(), new_customer_id, email_param, notifications_param);
end;

CREATE PROCEDURE create_device(IN new_device_id CHAR(36), IN machine_type_param CHAR(36),
                               IN machine_model_id_param CHAR(36),
                               in serial_number_param VARCHAR(255), IN equipment_number_param VARCHAR(255),
                               IN suffix_param VARCHAR(255), IN customer_id_param CHAR(36))
begin
    insert into devices
    values (new_device_id, machine_type_param, machine_model_id_param, serial_number_param, equipment_number_param,
            suffix_param, customer_id_param, FALSE, 'API', NOW());
    insert into device_stats
    values (UUID(), new_device_id, NULL, NULL,
            NULL, 0, 0, 0, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL);
end;

CREATE TRIGGER trg_before_insert_service_xpress_data
    BEFORE INSERT
    ON service_xpress_data
    FOR EACH ROW
BEGIN
    update service_xpress_data
    set discovery_code = CASE
                             when activity_type = 'INS_N' then 'INS'
                             when activity_type = 'DEI_N' then 'DEI'
        end
    where service_xpress_data.id = new.id;
end;


# create function fill_device_audit_data(machine_type_name varchar(255), machine_model_name VARCHAR(255),
#                                        device_id CHAR(36), serial_number VARCHAR(255),
#                                        equipment_number VARCHAR(255), customer_name varchar(255), customer_id CHAR(36))
#     returns VARCHAR(1024)
# begin
#     return concat_ws('_', machine_type_name, machine_model_name, device_id, serial_number, equipment_number,
#                      customer_name, customer_id);
# end;

create procedure create_device_audit_data(IN device_id CHAR(36), out audit_data varchar(255))
begin
    select concat_ws('_', machine_types.name, machine_models.name, devices.id, serial_number, equipment_number,
                     customers.name, customer_id)
    into audit_data
    from devices
             join customers on customer_id = customers.id
             join machine_models on machine_model_id = machine_models.id
             join machine_types on machine_types.id = machine_models.machine_type_id
    where devices.id = device_id;
end;


create trigger trg_before_delete_devices
    before delete
    on devices
    for each row
begin
    DECLARE action VARCHAR(255);
    DECLARE audit_data VARCHAR(1024);
    SET action = 'DELETE';

    call create_device_audit_data(old.id, audit_data);
    insert into audit_log values (uuid(), old.id, action, now(), audit_data, null);
end;

create trigger trg_before_update_devices
    before update
    on devices
    for each row
begin
    DECLARE action VARCHAR(255);
    DECLARE audit_data VARCHAR(1024);
    SET action = 'UPDATE';

    call create_device_audit_data(old.id, audit_data);
    insert into audit_log values (uuid(), old.id, action, now(), audit_data, null);
end;

create trigger trg_after_update_devices
    after update
    on devices
    for each row
begin
    DECLARE audit_data VARCHAR(1024);
    call create_device_audit_data(new.id, audit_data);
    update audit_log
    set affected_item_data_after = audit_data
    where affected_item = old.id
      and action_type = 'UPDATE'
      and affected_item_data_after is NULL;

    update device_stats
    set active_since=IFNULL(active_since, NOW())
    where device_id = old.id;
end;

