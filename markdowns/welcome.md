
You have the following tables provided

DEAL
-------------
|DEALID |NAME |
|-------|-----|
|1      |A    |
|2      |B    |
|3      |C    |
|4      |D    |
|5      |E    |
|6      |F    |

PRICE
--------------
DEALID |PRICE 
-------|------
1      |0,3   
2      |4     
3      |7     
2      |10    
5      |1,6   
6      |8     




Write a request that provide this output :

NAME |TOTAL 
-----|------
A    |0,3   
B    |14    
C    |7     
D    |0     
E    |1,6   
F    |8   


@[SQL ?]({"stubs": ["universe.sql"], "command": "com.yourself.H2Test#test"})
