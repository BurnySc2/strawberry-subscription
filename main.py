from typing import AsyncGenerator, Set

import uvicorn
from fastapi import FastAPI
from loguru import logger
import strawberry
from strawberry.fastapi import GraphQLRouter

from broadcaster import Broadcast

active_users: Set[str] = set()
broadcast = Broadcast()


@strawberry.type
class Query:
    @strawberry.field
    def hello(self) -> str:
        return 'Hello World'


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def chat_join_room(self, username: str) -> bool:
        if username in active_users:
            return False
        active_users.add(username)
        await broadcast.publish("chatroom", username)
        return True


@strawberry.type
class Subscription:
    @strawberry.subscription
    async def chat_user_joined(self) -> AsyncGenerator[str, None]:
        async with broadcast.subscribe(channel="chatroom") as subscriber:
            logger.info("subscribed")
            async for event in subscriber:
                logger.info(event)
                yield event.message
        logger.info("unsubscribed")


schema = strawberry.Schema(Query, mutation=Mutation, subscription=Subscription)

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix='/graphql')

if __name__ == '__main__':
    uvicorn.run('__main__:app', host='0.0.0.0', port=8000, reload=True)
