

from core import put_data , get_data_file
import io
# Upload file to s3


def upload_file():


    # binary_file = io.open('file.txt', 'rb')
    # text_file = io.TextIOWrapper(binary_file, encoding='utf-8', newline='')
    f = open("file.txt", "r")
 
    put_data(
        's3://staticfolders/file.txt',
        f.read()
    )

    print('done')


    print(get_data_file('s3://staticfolders/file.txt'))

upload_file()