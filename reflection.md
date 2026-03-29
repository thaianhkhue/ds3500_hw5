DS3500 HW 5 reflection.md
=======

**What does your animation show about the storm's impact?** 
- In animation A, the actual travel time spikes noticeably around Feb 23 and 24 while the scheduled line stays relatively flat (less than 250 sec difference between days)
- In Animation B, the clearest storm signal is gray (missing) cells at Andrew, Savin Hill, Fields Corner, and Shawmut on Feb 23-24, with JFK/UMass showing its highest travel time spike of the month, though most stops returned to baseline within a day or two.

**Data limitations**

In the cleaning function, rows with null scheduled_travel_time were dropped before computing scheduled trip totals. This means on Feb 23 - 24, the scheduled average in Animation A is computed from fewer trips than usual since the MBTA ran non standard service that didn't match the schedule. The scheduled line doesn't disappear on storm days, but it's less representative. It only reflects the trips that did match a planned schedule, which were likely the ones not as effected by the storm. This makes the gap between actual and scheduled look smaller in reality.

**Layered architecture**

When the service_date column came back as timestamps instead of readable dates, we were able to go back in and fix the issue in the acquire.py file. We only had to fix it in one place, and everything else updated. 

**AI Usage statement**

We used AI (Claude) to mainly help debug issues we were having with our environment when initially setting up the layers. We ran into pyarrow and Python release issues, and AI walked us through the errors. In addition, we also utilized it to build out the structure of the acquisition and model layers, before going in to specify parameters based on the assignment. We also used it to help figure out specific code issues like the correct LAMP URL format and working through the service_date parsing . We had to later verify the actual column names from the parquet schema and adjust the deduplication logic when stop_id wasn't present in the data.