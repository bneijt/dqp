Disk Queue Processing
=====================


Library to do simple disk based processing of messagepack dictionaries in a file.

All files are flat files and directories. To manage a simple folder structure with naming convention, use the `Project` class.

From the project you can open a source/sink and read/write with them using python dictionaries.

Sinks are rotated, sources keep track of a last read entry to allow you to continue later. To do this on close, use the Project class.

```
from dqp.queue import Project

with Project("/tmp/banana") as project:
    s = project.open_sink("hello")
    s.write_dict({"a": 1})
    s.write_dict({"b": 1})
    s.write_dict({"c": 1})
    s.write_dict({"d": 1})

with Project("/tmp/banana") as project:
    s = project.continue_source("hello")
    for filename, index, msg in s:
        print("1st go:", msg)
        break

with Project("/tmp/banana") as project:
    s = project.continue_source("hello")
    for filename, index, msg in s:
        print("2nd go:", msg)

```

Queue files are rotated based on timestamp, so each write_dict does look at the clock to see if we already need to rotate and what the new file path should be.

Clean up by telling the source to unlink everything up to last or a given queue filename prefix.
