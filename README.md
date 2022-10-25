# Test performance of Arrow IPC

Small repo to test performance of using Apache Arrow as exchange format for larger datasets. Two methods are compared: sending an Arrow table over HTTPs as a binary stream, and flushing the table to disk on a shared volume. 

To get it working, make sure you have Modal installed.

```
pip install modal-client
```

deploy both modal apps:

```
modal app deploy arrow_sender.py
modal app deploy arrow_receiver.py
```

Enter the base url generated for arrow_sender's fastapi client in arrow_receiver.py:

```PYTHON
# replace with generated for the arrow_sender app
sender_url = '<enter fast api web url>'
```

finally, test the peformance of both methods:

```
python test_performance.py
```



