import asyncio
from pprint import pprint
import aiohttp
import datetime
from dotenv import load_dotenv
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import JSON, Integer, Column, String


load_dotenv()
PG_DNS = f'postgresql+asyncpg://{os.environ.get("PG_USER")}:' \
         f'{os.environ.get("PG_PASS")}@{os.environ.get("PG_HOST")}:' \
         f'{os.environ.get("PG_PORT")}/{os.environ.get("PG_DB")}'
engine = create_async_engine(PG_DNS)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class SwapiPeople(Base):
    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


async def download_data(links, session):
    response_coros = [(await session.get(link)).json() for link in links]
    json_data_list = await asyncio.gather(*response_coros)
    return json_data_list


async def get_people(id, session):
    async with session.get(f'https://swapi.dev/api/people/{id}/') as response:
        json_data = await response.json()
        films = download_data(json_data.get('films'), session)
        homeworld = download_data([json_data.get('homeworld')], session)
        species = download_data(json_data.get('species'), session)
        starships = download_data(json_data.get('starships'), session)
        vehicles = download_data(json_data.get('vehicles'), session)
        list_data = await asyncio.gather(films, homeworld, species, starships, vehicles)
        hero = {
            'id': id,
            'birth_year': json_data.get('birth_year'),
            'eye_color': json_data.get('eye_color'),
            'films': ', '.join([film['title'] for film in list_data[0]]),
            'gender': json_data.get('gender'),
            'hair_color': json_data.get('hair_color'),
            'height': json_data.get('height'),
            'homeworld': ', '.join([home['name'] for home in list_data[1]]),
            'mass': json_data.get('mass'),
            'name': json_data.get('name'),
            'skin_color': json_data.get('skin_color'),
            'species': ', '.join([specie['name'] for specie in list_data[2]]),
            'starships': ', '.join([starship['name'] for starship in list_data[3]]),
            'vehicles': ', '.join([vehicle['name'] for vehicle in list_data[4]]),
        }
        # pprint(hero)
        return hero


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    async with aiohttp.ClientSession() as session:
        coros = [get_people(i, session) for i in range(1, 2)]
        results = await asyncio.gather(*coros)
    # print(results, sep='\n')
    for result in results:
        async with Session() as session:
            session.add(SwapiPeople(**result))
            await session.commit()


if __name__ == '__main__':
    start = datetime.datetime.now()
    # asyncio.run(main())
    asyncio.get_event_loop().run_until_complete(main())
    print(datetime.datetime.now() - start)
