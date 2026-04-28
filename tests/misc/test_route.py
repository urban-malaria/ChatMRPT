import sys
sys.path.insert(0, '/home/ec2-user/ChatMRPT')
import asyncio
from app.api.analysis.chat_routing import route_with_mistral

async def test():
    context = {'has_uploaded_files': True, 'csv_loaded': True, 'session_id': 'test'}
    result = await route_with_mistral('Plot me the map distribution for the evi variable', context)
    print(f'Result: {result}')
    
    result2 = await route_with_mistral('Tell me about the variables in my data', context)
    print(f'Result2: {result2}')

asyncio.run(test())
