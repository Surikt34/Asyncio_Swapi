import aiohttp
import asyncio
import time
from sqlalchemy.dialects.postgresql import insert
from models import SessionDB, Character, init_orm

API_URL = 'https://swapi.dev/api/people/'
MAX_REQUESTS = 20  # Количество одновременных запросов
semaphore = asyncio.Semaphore(MAX_REQUESTS)  # Ограничиваем количество запросов

# Асинхронный запрос к API с использованием семафора
async def fetch_character_data(session, url):
    async with semaphore:
        try:
            async with session.get(url, timeout=20) as response:
                return await response.json()
        except asyncio.TimeoutError:
            print(f"Запрос к {url} превысил время ожидания")
        except aiohttp.ClientError as e:
            print(f"Ошибка при выполнении запроса к {url}: {e}")
        return None

# Получение имен связанных объектов (фильмы, виды, корабли, транспорт)
async def fetch_names(urls, field_name, session):
    tasks = [fetch_character_data(session, url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    names = [data[field_name] for data in results if data and field_name in data]
    return names

# Получение названия планеты по ссылке
async def fetch_name(url, field_name, session):
    if url:
        data = await fetch_character_data(session, url)
        if data and field_name in data:
            return data[field_name]
    return None

# Сохранение данных персонажей в базу данных с использованием UPSERT
async def save_character(data, session, http_session):
    if 'url' not in data:
        print(f"Пропускаем запись без 'url': {data}")
        return

    films = await fetch_names(data.get('films', []), 'title', http_session)
    species = await fetch_names(data.get('species', []), 'name', http_session)
    starships = await fetch_names(data.get('starships', []), 'name', http_session)
    vehicles = await fetch_names(data.get('vehicles', []), 'name', http_session)
    homeworld = await fetch_name(data.get('homeworld'), 'name', http_session)

    character_id = int(data['url'].split('/')[-2])

    character_data = {
        'id': character_id,
        'name': data.get('name', 'Unknown'),
        'birth_year': data.get('birth_year', 'Unknown'),
        'eye_color': data.get('eye_color', 'Unknown'),
        'films': ', '.join(films),
        'gender': data.get('gender', 'Unknown'),
        'hair_color': data.get('hair_color', 'Unknown'),
        'height': data.get('height', 'Unknown'),
        'homeworld': homeworld or 'Unknown',
        'mass': data.get('mass', 'Unknown'),
        'skin_color': data.get('skin_color', 'Unknown'),
        'species': ', '.join(species),
        'starships': ', '.join(starships),
        'vehicles': ', '.join(vehicles)
    }

    stmt = insert(Character).values(**character_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['id'],
        set_=character_data
    )
    await session.execute(stmt)
    await session.commit()

# Асинхронная функция для обработки всех персонажей
async def fetch_all_characters():
    await init_orm()  # Инициализация базы данных
    async with aiohttp.ClientSession() as http_session:
        coros = [fetch_character_data(http_session, f'{API_URL}{i}/') for i in range(1, 101)]
        async with SessionDB() as session:
            results = await asyncio.gather(*coros, return_exceptions=True)  # Параллельные запросы
            for person in results:
                if isinstance(person, Exception):
                    print(f"Ошибка при получении данных: {person}")
                    continue
                if person:
                    await save_character(person, session, http_session)


if __name__ == "__main__":
    start_time = time.time()
    try:
        asyncio.run(fetch_all_characters())
    except KeyboardInterrupt:
        print("Программа была прервана")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Время выполнения программы: {elapsed_time:.2f} секунд")