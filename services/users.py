from .aws_resources import ic_numbers_table

def get_user_data(user_ic):
    response = ic_numbers_table.get_item(Key={'icNumber': user_ic})
    return response.get('Item', {})
