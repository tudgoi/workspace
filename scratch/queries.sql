CREATE VIEW state_code (code, name) AS
    SELECT substr(id,3), replace(name, 'Governor of ', '')
    FROM office
    WHERE id LIKE 'go%'
/* state_code(code,name) */;
insert into office (id, name) select concat('com', code), concat(name, ' Council of Ministers') from state_code where code != 'tn';
insert into supervisor (office_id, relation, supervisor_office_id) select concat('go', code), 'adviser', concat('com', code) from state_code where code != 'tn';
insert into supervisor(office_id, relation, supervisor_office_id) select concat('com',code), "head", concat('cmo',code) from state_code;