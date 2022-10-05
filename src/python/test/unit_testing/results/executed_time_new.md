# Execution time after using left anti join in load_core:


## Bearbnw:
```
before insert: 2019-10-01 16:28:15 
after insert: 2019-10-01 16:28:18 
```

## Hvgruppe:

```
before insert: 2019-10-01 16:27:10 
after insert: 2019-10-01 16:27:13 
```

## Lv: 

```
before insert: 2019-10-01 15:51:13
after insert: 2019-10-01 15:51:16 
```

## Jurlv:
```
before insert: 2019-10-01 16:29:06
after insert: 2019-10-01 16:29:09 
```

## Vt:
```
before insert: 2019-10-01 16:25:13
after insert: 2019-10-01 16:25:16
```

# Execution time on newly created empty tables:

## Bearbnw:

```
rows in rawvault of table BEARBNW 21751
rows in core of table BEARBNW 0
rows in temptable to be inserted from:  21751
before BEARBNW insert: 2019-10-02 10:27:52
after BEARBNW insert: 2019-10-02 10:27:55
```

## Hvgruppe:

```
rows in rawvault of table HVGRUPPE 3982
rows in core of table HVGRUPPE 0
rows in temptable to be inserted from:  3982
before HVGRUPPE insert: 2019-10-02 10:29:08
after HVGRUPPE insert: 2019-10-02 10:29:10
```

## Jurlv:

```
rows in rawvault of table JURLV 7019
rows in core of table JURLV 0
rows in temptable to be inserted from:  7019
before JURLV insert: 2019-10-02 10:30:25
after JURLV insert: 2019-10-02 10:30:28 
```

## Hvgruppe:
```
rows in rawvault of table LV 3933
rows in core of table LV 0
rows in temptable to be inserted from:  3933
before LV insert: 2019-10-02 10:31:40
after LV insert: 2019-10-02 10:31:42 
```

## Vt:
```
rows in rawvault of table VT 13206
rows in core of table VT 0
rows in temptable to be inserted from:  13206
before VT insert: 2019-10-02 10:33:07
after VT insert: 2019-10-02 10:33:11
```
# Note:

- Insertion times for when the core tables are not empty is when it's not mentioned that the tables are empty

- The join function exection time on the dataframe is Always instantaneous for every table whether or not the core table is empty (the below is when there is data in core):
```
before join: 2019-10-02 13:43:11
after join: 2019-10-02 13:43:11
```
The insertion time is also always between 2-3 seconds in duration as there is no comparison between table values, once again, whether or not the core tables have any data or not
```
rows in rawvault of table VT 26412
rows in core of table VT 13206
rows in temptable to be inserted from:  0
before VT insert: 2019-10-02 13:43:22
after VT insert: 2019-10-02 13:43:27
```
