FROM python:3

ADD fileTransfer.py /

RUN azcopy.exe

RUN pip3 install boto3

CMD [ "python3", "fileTransfer.py" ]

