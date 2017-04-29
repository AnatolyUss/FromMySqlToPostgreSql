drop table if exists zero;
create table zero (
ID bigint(20) NOT NULL,
DATA varchar(255) NOT NULL,
PRIMARY KEY (ID)
) ENGINE=InnoDB;

drop table if exists one;
create table one (
ID bigint(20) NOT NULL,
DATA varchar(255) NOT NULL,
PRIMARY KEY (ID),
CONSTRAINT one_FK1 FOREIGN KEY (ID) REFERENCES zero (ID)
) ENGINE=InnoDB;

drop table if exists two;
create table two (
ID bigint(20) NOT NULL,
one_ID bigint(20) NOT NULL,
DATA varchar(255) NOT NULL,
PRIMARY KEY (ID),
CONSTRAINT two_FK1 FOREIGN KEY (one_ID) REFERENCES one (ID)
) ENGINE=InnoDB;

drop table if exists three;
create table three (
ID bigint(20) NOT NULL,
one_ID bigint(20) NOT NULL,
DATA varchar(255) NOT NULL,
PRIMARY KEY (ID),
CONSTRAINT three_FK1 FOREIGN KEY (one_ID) REFERENCES one (ID)
) ENGINE=InnoDB;

insert into zero values ( 1, 'One' );
insert into one values ( 1, 'One' );
insert into two values ( 1, 1, 'Two' );
insert into three values ( 1, 1, 'Three' );
