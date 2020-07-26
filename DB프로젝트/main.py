import psycopg2 as pg2
import pandas as pd

def print_select(table_ , sql):
    print('<{}> table'.format(table_))
    cursor.execute(sql)
    rows = cursor.fetchall()
    df = pd.DataFrame(data=rows)
    df.columns = [column[0] for column in cursor.description]
    print(df)

#============Postgresql 연결=============
def psql_connect():

    #PostgreSQL 연동
    conn_string = """dbname = 'project_movie'  
                  user = 'postgres' 
                  host='127.0.0.1'  
                  port ='5432' 
                   password = 'cse3207' """
    conn = pg2.connect(conn_string)

    #Connection 으로부터 Cursor 생성
    cursor = conn.cursor()
    print('연결 성공!\n', 80 * "=")
    return conn, cursor
conn, cursor = psql_connect()


#===============================================================================
# 1. Create the tables and insert the proper data based on the provided data
def No_1_create_insert_base_table():
    #================테이블 생성===========
    #1. movie
    create_table_movie = """
                create table movie 
                    ( movieID varchar(30) ,
                     movieName varchar(30),
                     releaseYear numeric(4,0),
                     releaseMonth numeric(2,0),
                     releaseDate date,
                     publisherName varchar(30),
                    avgRate numeric(3,1),
                     primary key(movieID))"""
    cursor.execute(create_table_movie)

    #2. director
    create_table_director = """
    
                create table director  
                (directorID varchar(30),
                 directorName varchar(30) ,
                 dateOfBirth date,
                 dateOfDeath date,
                primary key(directorID))"""
    cursor.execute(create_table_director)


    #3. actor
    create_actor_table = """
            create table actor(
            actorID varchar(30),    
            actorName varchar(30),
            dateOfBirth date,
            dateOfDeath date,
            gender varchar(6)
                check (gender in ('Male','Female')),
            primary key(actorID))"""
    cursor.execute(create_actor_table)

    #4. customer
    create_customer_table = """
            create table customer(
            customerID varchar(30),
            customerName varchar(30),
            dateOfBirth date,
            gender varchar(6)
                check (gender in ('Male','Female')),
            primary key(customerID))"""
    cursor.execute(create_customer_table)

    #5. award
    create_award_table = """
            create table award(
               awardID varchar(30),
               awardName varchar(30),
               primary key(awardID))"""
    cursor.execute(create_award_table)

    #6. genre
    create_genre_table = """
            create table genre(
               genreName varchar(30),
               primary key(genreName))"""
    cursor.execute(create_genre_table)

    #7. movieGenre
    create_movieGenre_table = """
            create table movieGenre(
               movieID varchar(30),
               genreName varchar(30),
               primary key(movieID, genreName),
               foreign key (movieID) references movie
                  on delete cascade,
               foreign key (genreName) references genre)"""
    cursor.execute(create_movieGenre_table)

    #8. movieObtain
    create_movieObtain_table = """
            create table movieObtain(
               movieID varchar(30),
               awardID varchar(30),
               year numeric(4,0),
               primary key(movieID, awardID),
               foreign key(awardID) references award,
               foreign key(movieID) references movie)"""
    cursor.execute(create_movieObtain_table)

    #9. actorOBtain
    create_actorObtain_table = """
            create table actorObtain(
               actorID varchar(30),
               awardID varchar(30),
               year numeric(4,0),
               primary key(actorID, awardID),
               foreign key(actorID) references actor,
               foreign key(awardID) references award)"""
    cursor.execute(create_actorObtain_table)

    #10 . directorObtain
    create_directorObtain_table = """
            create table directorObtain(
               directorID varchar(30),
               awardID varchar(30),
               year numeric(4,0),
               primary key(directorID, awardID),
               foreign key(directorID) references director,
               foreign key(awardID) references award)"""
    cursor.execute(create_directorObtain_table)

    #11. casting
    create_casting_table = """
       create table casting(
       movieID varchar(30),
       actorID varchar(30),
       role varchar(30),
       primary key(movieID , actorID),
       foreign key(movieID) references movie
          on delete cascade,
       foreign key(actorID) references actor) """
    cursor.execute(create_casting_table)

    #12. make
    create_make_table = """
            create table make(
               movieID varchar(30),
               directorID varchar(30),
               primary key(movieID, directorID),
               foreign key(movieID) references movie
                  on delete cascade,
               foreign key(directorID) references director)"""
    cursor.execute(create_make_table)

    #13. customerRate
    create_customerRate_table = """
            create table customerRate(
               customerID varchar(30),
               movieID varchar(30),
               rate numeric(3,1),
               primary key(customerID , movieID),
               foreign key(customerID) references customer
                  on delete cascade,
               foreign key(movieID) references movie)"""
    cursor.execute(create_customerRate_table)

    #create trigger_func
    create_trigger_func = """
        CREATE OR REPLACE FUNCTION trigger_customerRate_insert_func()
        RETURNS TRIGGER
        LANGUAGE 'plpgsql'
    AS $BODY$
    BEGIN
      UPDATE movie m
         SET avgrate = ( SELECT AVG(c.rate) rate FROM customerRate c WHERE c.movieID = NEW.movieID )
       WHERE m.movieID = NEW.movieID;
      RETURN NEW;
    END;
    $BODY$;"""
    cursor.execute(create_trigger_func)

    #after insert trigger create
    trigger_after_insert = """
        CREATE TRIGGER trigger_customerRate_insert
        after INSERT
        ON customerRate
        FOR EACH ROW
        EXECUTE PROCEDURE public.trigger_customerRate_insert_func();"""
    cursor.execute(trigger_after_insert)

    #---------------------insert value----------------

    #1. insert_movie
    insert_into_movie = """
        INSERT INTO movie (movieID, movieName, releaseYear, releaseMonth, releaseDate, publisherName, avgRate) VALUES
                      ('Mov_1','Edward Scissorhands','1991','06','1991-06-29','20th Century Fox Presents', NULL),
                        ('Mov_2', 'Alice in Wonderland','2010','03','2010-03-04','Korea Sony Pictures', NULL),
                        ('Mov_3','The Social Network', '2010','11','2010-11-18','Korea Sony Pictures', NULL),
                        ('Mov_4','The Dark Knight',    '2008','08','2008-08-06','Warner Brothers Korea',NULL),
                        ('Mov_5','Dunkirk',            '2017','07','2017-07-13','Warner Brothers Korea',NULL);"""
    cursor.execute(insert_into_movie)

    #2. insert_director
    insert_into_director ="""
        insert into director (directorID, directorName , dateOfBirth , dateOfDeath) VALUES
                      ('Dir_1' ,'Tim Burton','1958-08-25', NULL),
                           ('Dir_2' ,'David Fincher','1962-08-28', NULL),
                           ('Dir_3', 'Christopher Nolan','1970-07-30', NULL);"""
    cursor.execute(insert_into_director)

    #3. insert_actor
    insert_into_actor = """
        insert into actor (actorID,actorName,dateOfBirth,dateOfDeath,gender) values
                   ('Act_1','Johnny Depp','1963,06,09',NULL,'Male'),
                         ('Act_2','Winona Ryder','1971-10-29',NULL,'Female'),
                         ('Act_3','Anne Hathaway','1982-11-12',NULL,'Female'),
                         ('Act_4','Christian Bale','1974-1-30',NULL,'Male'),
                         ('Act_5','Heath Ledger','1979-4-4', '2008-1-22','Male'),
                         ('Act_6','Jesse Eisenberg','1983-10-5',NULL,'Male'),
                         ('Act_7','Andrew Garfield','1983-8-20',NULL,'Male'),
                         ('Act_8','Fionn Whitehead','1997-7-18',NULL,'Male'),
                         ('Act_9','Tom Hardy','1977-9-15',NULL,'Male');"""
    cursor.execute(insert_into_actor)

    #4. insert customer
    insert_into_customer = """
    insert into customer(customerID,customerName,dateOfBirth,gender) values
                           ('Customer_1','Bob','1997.11.14','Male'),
                           ('Customer_2','John','1978.01.23','Male'),
                           ('Customer_3','Jack','1980.05.04','Male'),
                           ('Customer_4','Jill','1981.04.17','Female'),
                           ('Customer_5','Bell','1990.05.14','Female');"""
    cursor.execute(insert_into_customer)

    #5. insert_make
    insert_into_make = """
        insert into make (movieID , directorID) values
                ('Mov_1' , 'Dir_1'),
                ('Mov_2' , 'Dir_1'),
                ('Mov_3' , 'Dir_2'),
                ('Mov_4' , 'Dir_3'),
                ('Mov_5' , 'Dir_3');"""
    cursor.execute(insert_into_make)

    #6. insert_casting
    insert_into_casting = """
        insert into casting  (movieID , actorID , role) values
          ('Mov_1', 'Act_1' , 'Main actor'),
          ('Mov_1', 'Act_2' , 'Main actor'),
          ('Mov_2', 'Act_1' , 'Main actor'),
          ('Mov_2', 'Act_3' , 'Main actor'),
          ('Mov_3', 'Act_6' , 'Main actor'),
          ('Mov_3', 'Act_7' , 'Supporting actor'),
          ('Mov_4', 'Act_4' , 'Main actor'),
          ('Mov_4', 'Act_5' , 'Main actor'),
          ('Mov_5', 'Act_8' , 'Main actor'),
          ('Mov_5', 'Act_9' , 'Main actor');"""
    cursor.execute(insert_into_casting)

    #7. insert_genre
    insert_into_genre = """
    insert into genre(genreName) values
       ('Fantasy') , ('Romance') , ('Adventure'),('Family'),('Drama'),
       ('Action'), ('Mystery') , ('Thriller'), ('War');"""
    cursor.execute(insert_into_genre)

    #8. insert_movieGenre
    insert_into_movieGenre = """
        insert into moviegenre(movieID , genreName) values 
             ('Mov_1' , 'Fantasy'),
             ('Mov_1' , 'Romance'),
             ('Mov_2' , 'Fantasy'),
             ('Mov_2' , 'Adventure'),
             ('Mov_2' , 'Family'),
             ('Mov_3' , 'Drama'),
             ('Mov_4' , 'Action'),
             ('Mov_4' , 'Drama'),
             ('Mov_4' , 'Mystery'),
             ('Mov_4' , 'Thriller'),
             ('Mov_5' , 'Action'),
             ('Mov_5' , 'Drama'),
             ('Mov_5' , 'Thriller'),
             ('Mov_5' , 'War');"""
    cursor.execute(insert_into_movieGenre)
    print('모든 Table Created및,  Insert 완료!\n',80*'=')
No_1_create_insert_base_table()

#================================================================================
# 2번 문제
def Insert_2_1():
    # 2.1 Winona Ryder won the "BEst supporting actor" award in 1994
    print('2.1 Winona Ryder won the "Best supporting actor" award in 1994')
    No_2_1_sql_insert= "insert into award values('1' , 'Best supporting actor')"
    cursor.execute(No_2_1_sql_insert)
    print('Translated SQL : ' , No_2_1_sql_insert)
    Show_2_1_sql = "select * from award"
    print_select(table_ = 'award' , sql = Show_2_1_sql)

    No_2_1_sql_insert = "insert into actorObtain values('Act_2','1',1994)"
    cursor.execute(No_2_1_sql_insert)
    print('Translated SQL : ' , No_2_1_sql_insert )
    Show_2_1_sql = "select * from actorObtain"
    print_select(table_ = 'actorObtain' , sql = Show_2_1_sql)
Insert_2_1()

def Insert_2_2():
    # 2.2 Andrew Garfield won the "Best Supporting actor" award in 2011
    print('\n2.2 Andrew Garfield won the "Best Supporting actor" award in 2011')
    No_2_2_sql_insert = "insert into actorObtain values('Act_7' , '1','2011')"
    cursor.execute(No_2_2_sql_insert)
    print('Translated SQL : ' , No_2_2_sql_insert )
    Show_2_2_sql = "select * from actorObtain"
    print_select(table_ = 'actorObtain' , sql = Show_2_2_sql)
Insert_2_2()

def Insert_2_3():
    # Jessie Eisenberg won the "Best main actor"award in 2011
    print('\n2.3 Jessie Eisenberg won the "Best main actor"award in 2011')
    No_2_3_sql_insert= "insert into award values('2' , 'Best main actor')"
    cursor.execute(No_2_3_sql_insert)
    print('Translated SQL : ' , No_2_3_sql_insert)
    Show_2_3_sql = "select * from actor where actorName = 'Jesse Eisenberg'"
    print_select(table_ = 'actor' , sql = Show_2_3_sql)

    No_2_3_sql_insert = "insert into actorObtain values ('Act_6','2',2011)"
    cursor.execute(No_2_3_sql_insert)
    print('Translated SQL : ' , No_2_3_sql_insert )
    Show_2_3_sql = "select * from actorObtain"
    print_select(table_ = 'actorObtain' , sql = Show_2_3_sql)
Insert_2_3()

def Insert_2_4():
    # Johnny Depp won the "Best villian actor" award in 2011
    print('\n2.4 Johnny Depp won the "Best villian actor" award in 2011')
    No_2_4_sql_insert= "insert into award values('3' , 'Best villain actor')"
    cursor.execute(No_2_4_sql_insert)
    print('Translated SQL : ' , No_2_4_sql_insert)
    Show_2_4_sql = "select * from actor where actorName = 'Johnny Depp'"
    print_select(table_ = 'actor' , sql = Show_2_4_sql)

    No_2_4_sql_insert = "insert into actorObtain values ('Act_1','3',2011)"
    cursor.execute(No_2_4_sql_insert)
    print('Translated SQL : ' , No_2_4_sql_insert )
    Show_2_4_sql = "select * from actorObtain"
    print_select(table_ = 'actorObtain' , sql = Show_2_4_sql)
Insert_2_4()

def Insert_2_5():
    # Edward Scissorhands won the "Best fantasy movie" award in 1991
    print('\n2.5 Edward Scissorhands won the "Best fantasy movie" award in 1991')
    No_2_5_sql_insert= "insert into award values ('4' , 'Best fantasy movie')"
    cursor.execute(No_2_5_sql_insert)
    print('Translated SQL : ' , No_2_5_sql_insert)
    Show_2_5_sql = "select * from movie where movieName = 'Edward Scissorhands'"
    print_select(table_ = 'movie' , sql = Show_2_5_sql)

    No_2_5_sql_insert = "insert into movieObtain values('Mov_1' , '4' , 1991)"
    cursor.execute(No_2_5_sql_insert)
    print('Translated SQL : ' , No_2_5_sql_insert )
    Show_2_5_sql = "select * from movieObtain"
    print_select(table_ = 'movieObtain' , sql = Show_2_5_sql)
Insert_2_5()

def Insert_2_6():
    # The Dark Knight won the "Best Picture" award in 2009
    print('\n2.6 The Dark Knight won the "Best Picture" award in 2009')
    No_2_6_sql_insert= "insert into award values ('5' , 'Best picture')"
    cursor.execute(No_2_6_sql_insert)
    print('Translated SQL : ' , No_2_6_sql_insert)
    Show_2_6_sql = "select * from movie where movieName = 'The Dark Knight'"
    print_select(table_ = 'movie' , sql = Show_2_6_sql)

    No_2_6_sql_insert = "insert into movieObtain values('Mov_4', '5' , 2009)"
    cursor.execute(No_2_6_sql_insert)
    print('Translated SQL : ' , No_2_6_sql_insert )
    Show_2_6_sql = "select * from movieObtain"
    print_select(table_ = 'movieObtain' , sql = Show_2_6_sql)
Insert_2_6()

def Insert_2_7():
    # Alice Wonderland won the "Best fantasy movie" award in 2011
    print('\n2.7 Alice Wonderland won the "Best fantasy movie" award in 2011')
    Show_2_7_sql = "select * from movie where movieName = 'Alice in Wonderland'"
    print_select(table_ = 'movie' , sql = Show_2_7_sql)

    No_2_7_sql_insert = "insert into movieObtain values('Mov_2', '4' , 2011)"
    cursor.execute(No_2_7_sql_insert)
    print('Translated SQL : ' , No_2_7_sql_insert )
    Show_2_7_sql = "select * from movieObtain"
    print_select(table_ = 'movieObtain' , sql = Show_2_7_sql)
Insert_2_7()

def Insert_2_8():
    # David Fincher won the "Best director" award in 2011
    print('\n2.8 David Fincher won the "Best director" award in 2011')
    No_2_8_sql_insert = "insert into award values('6', 'Best director')"
    cursor.execute(No_2_8_sql_insert)
    print('Translated SQL : ', No_2_8_sql_insert)
    Show_2_8_sql = "select * from director where directorName = 'David Fincher'"
    print_select(table_='director', sql=Show_2_8_sql)

    No_2_8_sql_insert = "insert into directorObtain values('Dir_2','6', 2011)"
    cursor.execute(No_2_8_sql_insert)
    print('Translated SQL : ', No_2_8_sql_insert)
    Show_2_8_sql = "select * from directorObtain"
    print_select(table_='directorObtain', sql=Show_2_8_sql)
Insert_2_8()


#3번문제
def Select_3_1():
    print('\n\n3.1 Bob rates 5 to "The Dark Knight')
    No_3_insert_sql = """
        insert into customerRate(customerID , movieID , rate)
       select customerID , movieID , '5'
       from movie,customer where movie.movieName = 'The Dark Knight' 
                and customer.customerName = 'Bob'"""
    print('Translated SQL : ', No_3_insert_sql)
    cursor.execute(No_3_insert_sql)
    print_select(table_ = 'customerRate', sql = "select * from customerRate")
Select_3_1()

def Select_3_2():
    print('\n\n3.2 Bell rates 5 to the movies whose director is "Tim Burton"')
    No_3_2_insert_sql = """
        insert into customerRate(customerID, movieID, rate)
           select customerID, movieID, '5'
           from (select * from make natural join director) as T, customer
           where T.directorName = 'Tim Burton' 
              and customer.customerName = 'Bell' """
    print('Translated SQL : ',  No_3_2_insert_sql)
    cursor.execute(No_3_2_insert_sql)
    print_select(table_ = 'customerRate', sql = "select * from customerRate")
Select_3_2()

def Select_3_3():
    print('\n\n3.3 Jill rates 4 to the movies whose main actor is female ')
    No_3_insert_sql = """
        insert into customerRate(customerID, movieID, rate)
           select customerID , movieID , '4'
           from (select * from casting natural join actor) as T ,  customer
           where T.role = 'Main actor' and 
                T.gender = 'Female' and
                customer.customerName = 'Jill' """
    print('Translated SQL : ', No_3_insert_sql)
    cursor.execute(No_3_insert_sql)
    print_select(table_ = 'customerRate', sql = "select * from customerRate")
Select_3_3()

def Select_3_4():
    print('\n\n3.4 Jack rates 4 to the fantasy movies ')
    No_3_insert_sql = """
        insert into customerRate(customerID , movieID , rate)
           select customerID, movieID , '4'
           from (select * from moviegenre natural join movie) as T , customer
           where T.genrename = 'Fantasy' and
               customer.customerName = 'Jack' """
    print('Translated SQL : ', No_3_insert_sql)
    cursor.execute(No_3_insert_sql)
    print_select(table_ = 'customerRate', sql = "select * from customerRate")
Select_3_4()

def Select_3_5():
    print('\n\n3.5 John rates 5 to the movies whose director won the "Best director" award')
    No_3_insert_sql = """
      insert into customerRate(customerID, movieID, rate)
       select customerID , movieID , '5'
       from (select * from movie natural join make) as T  , customer
       where T.directorid = 'Dir_2' and
          customer.customerName = 'John'"""
    print('Translated SQL : ', No_3_insert_sql)
    cursor.execute(No_3_insert_sql)
    print_select(table_ = 'customerRate', sql = "select * from customerRate")
Select_3_5()

#4
def Select_4():
    print('\n\n4. Select the names of the movies whose actor are dead')
    No_4_sql = """select movieName from (actor natural join casting natural join movie) as T
                     where T.dateOfDeath is not null"""
    print('Translated SQL : ' , No_4_sql)
    cursor.execute(No_4_sql)
    print_select(table_ = 'movieName' , sql = No_4_sql)
Select_4()

def Select_5():
    print('\n\n5. Select the Names of the directors who cast the same actor mote than once')
    No_5_sql = """
    select directorName from (select directorName, count(actorID) 
   from make natural join casting natural join director group by (directorName, actorID)) as T
   where T.count >=2
    """
    print('Translated SQL : ' , No_5_sql)
    cursor.execute(No_5_sql)
    print_select(table_ = 'directorName' , sql = No_5_sql)
Select_5()

def Select_6():
    print('\n\n6. Select the names of the movies and the genres , \
                where movies have the common genre.')
    No_6_sql = """select genreName , movieName from movieGenre natural 
                    join movie group by (genreName , movieName)
                     order by genreName"""
    print('Translated SQL : ' , No_6_sql)
    cursor.execute(No_6_sql)
    print_select(table_ = 'movieGenre' , sql = No_6_sql)
Select_6()

def Delete_7():
    print('\n\n7. Delete the movies which did not get any award \n\
            (including all directors and actors)and delete data from related tables')
    No_7_sql = """(select movieID from movie natural join movieObtain) union 
    (select movieID from directorObtain natural join make natural join movie)union
    (select movieID from actorObtain natural join casting natural join movie)
    """
    cursor.execute("""(select movieID from movie natural join movieObtain) union 
    (select movieID from directorObtain natural join make natural join movie)union
    (select movieID from actorObtain natural join casting natural join movie)
    """)
    print_select(table_ = 'Union' , sql = No_7_sql)
    print('위는 배우 , 감독, 영화상 수상받은 영화를 합집합을 통해 나타냄. \n\
          따라서 Mov_5가 수상하지 못했음을 확인하고 Mov_5 제거')

    No_7_sql ="delete from movie where movieID = 'Mov_5'"
    print('Translated SQL : ', No_7_sql)
    cursor.execute(No_7_sql)
    No_7_sql_select = "select * from movie"
    print_select(table_ = 'movie' , sql = No_7_sql_select)

Delete_7()

def Delete_8():
    print('\n\n8. Delete all customers and delete data from related tables delete from customer')
    No_8_sql = "delete from customer"
    cursor.execute(No_8_sql)
    print('Translated SQL : ' , No_8_sql)
    No_8_sql_select = "select * from customer"
    print('customer 테이블 삭제 완료')

Delete_8()


def Delete_9():
    print('\n\n9. Delete all tables and data')
    No_9_sql = """
        drop table 
             actor , actorObtain , award , casting , customer ,customerRate,
             director , directorObtain , genre, make , 
             movie , movieGenre , movieObtain cascade"""
    cursor.execute(No_9_sql)
    print('Translated SQL : ', No_9_sql)
    print('모든 테이블 삭제가 완료되었습니다 .')

Delete_9()

conn.commit() #저장 역할

conn.close() #연결 종료