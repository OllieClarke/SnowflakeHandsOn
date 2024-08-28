//set database
use database library_card_catalog;

//set schema
use schema public;

//create sequence for author id
create sequence SEQ_AUTHOR_UID
     start = 1
     increment = 1
     ORDER
     comment = 'Use this to fill in Author_UID';

use role sysadmin;

//see how the nextvalfunction works
select seq_author_uid.nextval;

show sequences;

select seq_author_uid.nextval, seq_author_uid.nextval;

use role sysadmin;

//Drop and recreate the counter (sequence) so that it starts at 3 
// then we'll add the other author records to our author table
create or replace sequence library_card_catalog.public.seq_author_uid
start = 3 
increment = 1 
ORDER
comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

//Add the remaining author records and use the nextval function instead 
//of putting in the numbers
insert into author(author_uid,first_name, middle_name, last_name) 
values
(seq_author_uid.nextval, 'Laura', 'K','Egendorf')
,(seq_author_uid.nextval, 'Jan', '','Grover')
,(seq_author_uid.nextval, 'Jennifer', '','Clapp')
,(seq_author_uid.nextval, 'Kathleen', '','Petelinsek');

use database library_card_catalog;
use role sysadmin;

// Create a new sequence, this one will be a counter for the book table
create or replace sequence library_card_catalog.public.seq_book_uid
  start = 1
  increment = 1
  ORDER
  comment = 'Use this to fill in the BOOK_UID every time you add a new row';


// Create the book table and use the NEXTVAL as the 
// default value each time a row is added to the table

create or replace table book
( book_uid number default seq_book_uid.nextval
 , title varchar(50)
 , year_published number(4,0)
);

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the default setting
// will take care of it for you

insert into book(title, year_published)
values
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

// Create the relationships table
// this is sometimes called a "Many-to-Many table"
create table book_to_author
( book_uid number
  ,author_uid number
);

//Insert rows of the known relationships
insert into book_to_author(book_uid, author_uid)
values
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek


//Check your work by joining the 3 tables together
//You should get 1 row for every author
select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 
