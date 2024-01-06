import asyncio
import aiohttp

from more_itertools import chunked
from models import init_db, close_db, Person, Session

CHUNK_SIZE = 10
TOTAL = 71


async def get_person(person_id):
    session = aiohttp.ClientSession()
    get_coro = session.get(f"https://swapi.dev/api/people/{person_id}/")
    response = await get_coro
    json_response = await response.json()
    await session.close()
    return json_response


async def insert_people(people_list):
    to_add = []
    for person in people_list:
        people_list_new = Person(
            name=person.get("name", "-"),
            birth_year=person.get("birth_year"),
            eye_color=person.get("eye_color"),
            gender=person.get("gender"),
            hair_color=person.get("hair_color"),
            height=person.get("height"),
            mass=person.get("mass"),
            skin_color=person.get("skin_color"),
            homeworld=person.get("homeworld"),
            url=person.get("url"),
            films=person.get("films"),
            species=person.get("species"),
            starships=person.get("starships"),
            vehicles=person.get("vehicles"),
        )
        to_add.append(people_list_new)

    async with Session() as session:
        session.add_all(to_add)
        await session.commit()


async def main():
    await init_db()
    for person_id_chunk in chunked(range(1, TOTAL), CHUNK_SIZE):
        coros = [get_person(person_id) for person_id in person_id_chunk]
        result = await asyncio.gather(*coros)
        print(result)
        asyncio.create_task(insert_people(result))
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks)
    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
    print("Ok")
