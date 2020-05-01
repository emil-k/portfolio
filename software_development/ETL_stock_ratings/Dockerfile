FROM python:3.6-buster
COPY starting.py /my_project/
COPY requirements.txt /my_project/
WORKDIR /my_project/
RUN pip install -r requirements.txt
CMD [ "python", "./starting.py 'DE0005772206'" ]



