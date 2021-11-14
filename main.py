import asyncio
from typing import AsyncGenerator, Set

import uvicorn
from fastapi import FastAPI
from loguru import logger
import strawberry
from strawberry.fastapi import GraphQLRouter

active_users: Set[str] = set()
queue_user_joined: 'asyncio.Queue[str]' = asyncio.Queue()


@strawberry.type
class Query:
    @strawberry.field
    def hello(self) -> str:
        return 'Hello World'


@strawberry.type
class Mutation:
    @strawberry.mutation
    def chat_join_room(self, username: str) -> bool:
        if username in active_users:
            return False
        active_users.add(username)
        queue_user_joined.put_nowait(username)
        return True


@strawberry.type
class Subscription:
    @strawberry.subscription
    async def hello(self) -> AsyncGenerator[str, None]:
        count = 0
        while 1:
            yield f'Subscription message {count}'
            count += 1
            await asyncio.sleep(1)

    @strawberry.subscription
    async def chat_user_joined(self) -> AsyncGenerator[str, None]:
        while 1:
            if queue_user_joined.empty():
                await asyncio.sleep(0.01)
            else:
                new_user = queue_user_joined.get_nowait()
                logger.info(f'new user: {new_user}')
                yield new_user


schema = strawberry.Schema(Query, mutation=Mutation, subscription=Subscription)

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix='/graphql')

if __name__ == '__main__':
    uvicorn.run('__main__:app', host='0.0.0.0', port=8000, reload=True)
