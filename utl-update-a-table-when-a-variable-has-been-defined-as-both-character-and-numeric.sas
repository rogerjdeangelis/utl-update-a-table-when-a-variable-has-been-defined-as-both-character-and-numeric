Update master table when transaction table has conflicting types

We make the assumption that the master table has the correct attributes.

PROBLEM
=======

Update master table when transaction table has conflicting types.

            Master   Transaction
Variable    Table      Table

NAME        Char       Char     same primary key
WEIGHT      Char       Num      dif
HEIGHT      Num        Char     dif
AGE         Num        Char     dif

github
https://tinyurl.com/yazrwfbg
https://github.com/rogerjdeangelis/utl-update-a-table-when-a-variable-has-been-defined-as-both-character-and-numeric

github
https://tinyurl.com/y8r4rwd4
https://github.com/rogerjdeangelis/utl_append_tables_when_the_same_variables_have_different_types_and_lengths

SAS Forum
https://tinyurl.com/yawa9ven
https://communities.sas.com/t5/New-SAS-User/Error-Variable-has-been-defined-as-both-character-and-numeric/m-p/524899

utl_gather macro from
Alea Iacta
https://github.com/clindocu

* SQL dictionaries are often is often too slow,especially on non-programmers servers;
* You can use 'proc contents instead of utl_gather, but utl_gather has more functionality;
* I need to retype variables in the transaction dataset to match the master;

INPUT
=====

* MAKE DATA;

data transaction;
  set sashelp.class(obs=5 rename=(age=agen height=heightn));
  height=put(100*heightn,5.2);
  age=put(10*agen,5.);
  drop agen heightn;
run;quit;


data Master;
  set sashelp.class(obs=5 rename=(weight=weightn));
  weight=put(weightn,5.2);
  drop weightn;
run;quit;


WORK.MASTER total obs=4

 Variables in Creation Order

 Variable    Type    Len  |  RULES
                          |
 NAME        Char      8  |  ** same in master and transaction
 WEIGHT      Char      5  |  ** char in master and num in transaction need to convert transaction to car
 HEIGHT      Num       8  |  ** num in master and char in transaction need to convert transaction to num
 AGE         Num       8  |  ** num in master and char in transaction need to convert transaction to num
                          |

WORK.TRANSACTION

 Variables in Creation Order

 Variable    Type    Len

 NAME        Char      8
 WEIGHT      Num       8
 HEIGHT      Char      5
 AGE         Char      5

Types need to match master


EXAMPLE OUTPUT
==============

 WORK.WANT total obs=5

         Key      From Master    From Transaction
        ====      ============    =============
Obs     NAME      SEX   WEIGHT    AGE    HEIGHT

 1     Alfred      M    112.5     140     6900
 2     Alice       F     84.0     130     5650
 3     Barbara     F     98.0     130     6530
 4     Carol       F    102.5     140     6280
 5     Henry       M    102.5     140     6350


PROCESS
=======

  * get meta data only need one ob - sql dictionaaries are two slow;

  %utl_gather(master(obs=1),var,val,,masterXpo,valformat=$8.,WithFormats=Y);

  /*
  WORK.MASTERXPO total obs=4

   VAR       VAL       _COLFORMAT    _COLTYP

   NAME      Alfred     $8.             C
   AGE       14         BEST12.         N
   HEIGHT    69         BEST12.         N
   WEIGHT    112.5      $5.             C
  */

  %utl_gather(transaction(obs=1),var,val,,transactionXpo,valformat=$8.,WithFormats=Y);

  /*
  WORK.TRANSACTIONXPO total obs=4

    VAR       VAL       _COLFORMAT    _COLTYP

    NAME      Alfred     $8.             C
    WEIGHT    112.5      BEST12.         N
    HEIGHT    69.00      $5.             C
    AGE       14         $5.             C
  */

  proc sql;
   select
      case
        when l._coltyp = "N" then catx(" ","input(",l.var,",best12.) as",l.var)
        else catx(" ","put(",l.var,",5.1) as",l.var)
      end as chgTyp
   into
      :varChg  separated by ","
   from
      masterXpo as l, transactionXpo as r
   where
      l.var = r.var  and
      l._coltyp ne r._coltyp
   ;quit;

 /*
 input( AGE ,best12.) as AGE
 input( HEIGHT ,best12.) as HEIGHT
 put( WEIGHT ,5.1) as WEIGHT
*/

* merge will substitute the values in the master with the transaction but
  keep the lengths in the master;


* correct the types in the transaction to match the master;
proc sql;
  create
     view trnFix as
  select
     name
    ,&varChg
  from
     transaction
;quit;

* Update problematic variables. Assumes only probematic variables need updating
data want;
  merge master(in=m) trnFix;
  by name;
  if m;
run;quit;

OUTPUT
======

see above

