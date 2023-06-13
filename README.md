# ChangeRCA 

ChangeRCA is a difference in difference (DiD) and difference in difference in difference (DDD) based root cause change indetification framework

## Files of ChangeRCA

```
- difference.py: cal DiD or DDD result for pre-change and post-change instance
- log.py: record logs
- main.py: run changerca
- ranker.py: ranking final socre of each case
- util: some tool 
```

## Execute ChangeRCA
given a example input in `./data`, we can run changeRCA
```
python3 main.py
```

the reuslt will show in `./log`
