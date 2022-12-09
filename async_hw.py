import asyncio
from aiohttp import ClientSession

from more_itertools import chunked

from requests import get

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text


PG_DSN = 'postgresql+asyncpg://asyntest:asyncpasws@127.0.0.1:5431/db_app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()


class Person(Base):

    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    id_swapi = Column(Integer, nullable=False)
    name = Column(String(50), nullable=False)
    gender = Column(String(50), nullable=False)
    birth_year = Column(String(50), nullable=False)
    height = Column(String(50), nullable=False)
    mass = Column(String(50), nullable=False)
    hair_color = Column(String(50), nullable=False)
    eye_color = Column(String(50), nullable=False)
    skin_color = Column(String(50), nullable=False)
    homeworld = Column(String(50), nullable=False)
    films = Column(Text)
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)


def get_people_amount():
    return get('https://swapi.dev/api/people').json()['count']


async def get_person(person_id: int, session: ClientSession):
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        if response.status == 200:
            json_data = await response.json()
            json_data['id'] = person_id
            async with session.get(json_data['homeworld']) as response_hw:
                hw_dict = await response_hw.json()
                json_data['homeworld'] = hw_dict['name']
            films_list = []
            for film in json_data['films']:
                async with session.get(film) as response_film:
                    film_dict = await response_film.json()
                    films_list.append(film_dict['title'])
            json_data['films'] = films_list
            species_list = []
            for spec in json_data['species']:
                async with session.get(spec) as response_spec:
                    spec_dict = await response_spec.json()
                    species_list.append(spec_dict['name'])
            json_data['species'] = species_list
            starships_list = []
            for ship in json_data['starships']:
                async with session.get(ship) as response_ship:
                    ship_dict = await response_ship.json()
                    starships_list.append(ship_dict['name'])
            json_data['starships'] = starships_list
            vehicles_list = []
            for vehicle in json_data['vehicles']:
                async with session.get(vehicle) as response_vehicle:
                    vehicle_dict = await response_vehicle.json()
                    vehicles_list.append(vehicle_dict['name'])
            json_data['vehicles'] = vehicles_list
            return json_data


async def get_people(people_amount: int):
    if 0 < people_amount <= 10:
        chunk_size = people_amount
    elif people_amount > 10:
        chunk_size = 10
    else:
        raise ValueError('No people to process')
    async with ClientSession() as session:
        for chunk in chunked(range(1, people_amount+1), chunk_size):
            coroutines = [get_person(person_id=i, session=session) for i in chunk]
            result = await asyncio.gather(*coroutines)
            for item in result:
                if item:
                    yield item


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    people_amount = get_people_amount()
    async for person in get_people(people_amount):
        async with Session() as session:
            add_person = Person(id_swapi=person['id'],
                                name=person['name'],
                                gender=person['gender'],
                                birth_year=person['birth_year'],
                                height=person['height'],
                                mass=person['mass'],
                                hair_color=person['hair_color'],
                                eye_color=person['eye_color'],
                                skin_color=person['skin_color'],
                                homeworld=person['homeworld'],
                                films=', '.join(person['films']),
                                species=', '.join(person['species']),
                                starships=', '.join(person['starships']),
                                vehicles=', '.join(person['vehicles'])
                                )
            session.add(add_person)
            await session.commit()


asyncio.run(main())
