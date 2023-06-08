from datetime import datetime, timedelta, date
from typing import Optional

from fastapi import Query, APIRouter
from sqlalchemy import distinct

from app.database import session
from app.models import BookingBronIncrement, ClassBronSeason, RaspScoreAll, SeasonMosSochi, Season
from app.utils import (
    process_result_dynamic_single_data,
    process_result_dynamic_multiple_data,
    process_result_demand_forecast_data,
    process_result_season_data,
)

router = APIRouter(prefix='/api/v1/calculation', tags=['calculation'])


@router.get("/booking-dynamics")
async def get_booking_dynamics(
        flight_number: int = Query(..., description="Номер рейса", example="1120"),
        flight_date: str = Query(..., description="Дата рейса", example="2018-05-29"),
        booking_class: str = Query(..., description="Класс бронирования", example="Y"),
        booking_period: Optional[int] = Query(1, ge=1, le=12,
                                              description="Период прогнозирования спроса для рейса (в месяцах)",
                                              example='1'),
):
    """
    Определение динамики бронирований рейса в разрезе
    классов бронирования по вылетевшим рейсам.
    """
    try:
        booking_classes = booking_class.replace(" ", "").split(",")

        flight_date_obj = datetime.strptime(flight_date, "%Y-%m-%d").date()

        booking_period_start_date = flight_date_obj - timedelta(days=booking_period * 30)
        booking_period_end_date = flight_date_obj

        query = session.query(
            BookingBronIncrement.SDAT_S,
            BookingBronIncrement.Increment_day,
            BookingBronIncrement.PASS_BK,
        )

        query = query.filter(
            BookingBronIncrement.FLT_NUM == flight_number,
            BookingBronIncrement.DD == flight_date,
            BookingBronIncrement.SEG_CLASS_CODE.in_(booking_classes),
            BookingBronIncrement.SDAT_S.between(booking_period_start_date, booking_period_end_date),
            BookingBronIncrement.DTD >= 0,

        )

        dates_receipt = []
        increments_days = []
        for result in query.all():
            sdat_s = result.SDAT_S
            increments_day = result.Increment_day

            increments_days.append(increments_day)
            dates_receipt.append(sdat_s)

        if not dates_receipt or not increments_days:
            return {'status': 400, 'error': 'Некорректные данные. Один или несколько списков пустые.'}

        series_data = [{'series': []}]

        for booking_class in booking_classes:

            pass_query = query.filter(
                BookingBronIncrement.SEG_CLASS_CODE == booking_class,
                BookingBronIncrement.DTD >= 0,
            )

            pass_bks = [result.PASS_BK for result in pass_query.all()]

            if pass_bks:
                series_data[0]['series'].append({
                    'name': f'Суммарное бронирование {booking_class}',
                    'type': 'line',
                    'data': pass_bks
                })
            else:
                series_data[0]['series'].append({
                    'name': f'Данных для класс {booking_class} не найдено',
                    'type': 'line',
                    'data': [0, 0, 0, 0, 0, 0, 0, 0]
                })

        if len(booking_classes) == 1:
            series_data[0]['series'].append({
                'name': 'Бронирование за день',
                'type': 'column',
                'data': increments_days
            })
            res_data = process_result_dynamic_single_data(series_data, dates_receipt)
        else:
            res_data = process_result_dynamic_multiple_data(series_data, dates_receipt)

        return {'status': 200, "data": res_data}

    except Exception as e:
        return {'status': 500, "error": str(e)}


@router.get("/seasonality")
async def get_seasonality(
        direction: str = Query(..., description="Направление рейса", example="Москва - Сочи"),
        flight_number: str = Query(..., description="Номер рейса", example="1120"),
        booking_class: str = Query(..., description="Класс бронирования", example="Y"),
        booking_start: Optional[date] = Query(...,
                                              description="Период для просмотра стартовая дата",
                                              example='2018-05-29'),
        booking_end: Optional[date] = Query(...,
                                            description="Период для просмотра конечная дата",
                                            example='2019-12-31')
):
    """
    Определение динамики бронирований рейса в разрезе
    классов бронирования по вылетевшим рейсам.
    """
    try:
        series_data = [{'series': []}]

        class_bron_season_query = session.query(
            ClassBronSeason.SDAT_S,
            ClassBronSeason.Increment_day,
        )

        class_bron_season_query = class_bron_season_query.filter(
            ClassBronSeason.FLT_NUM == flight_number,
            ClassBronSeason.SEG_CLASS_CODE == booking_class,
            ClassBronSeason.SDAT_S.between(booking_start, booking_end)
        )

        dates_receipt = []
        increments_days = []

        for result in class_bron_season_query.all():
            sdat_s = result.SDAT_S
            increment_day = result.Increment_day

            dates_receipt.append(sdat_s)
            increments_days.append(increment_day)

        series_data[0]['series'].append({
            "name": "График спроса",
            "type": "line",
            "data": increments_days,
        })

        season_query = (
            session
            .query(distinct(Season.Season_name))
            .filter(Season.date_season.between(booking_start, booking_end))
        )

        seasons_names = [season_name[0] for season_name in season_query]

        for season_name in seasons_names:
            query = (
                session
                .query(Season.Height)
                .filter(
                    Season.date_season.between(booking_start, booking_end),
                    Season.Season_name == season_name,
                    Season.Direction == direction,
                )
                .all()
            )

            heights = [row[0] for row in query]

            if any(elem > 0 for elem in heights):
                series_data[0]['series'].append({
                    "name": season_name,
                    "type": "column",
                    "data": heights,
                })

        res_data = process_result_season_data(series_data, dates_receipt)

        return {'status': 200, 'data': res_data}

    except Exception as e:
        return {'status': 500, 'error': str(e)}


# @router.get("/seasonality")
# async def get_seasonality(
#         direction: str = Query(..., description="Направление рейса", example="Москва - Сочи"),
#         flight_number: str = Query(..., description="Номер рейса", example="1120"),
#         booking_class: str = Query(..., description="Класс бронирования", example="Y"),
#         booking_start: Optional[date] = Query(...,
#                                               description="Период для просмотра стартовая дата",
#                                               example='2018-05-29'),
#         booking_end: Optional[date] = Query(...,
#                                             description="Период для просмотра конечная дата",
#                                             example='2019-12-31')
# ):
#     """
#     Определение динамики бронирований рейса в разрезе
#     классов бронирования по вылетевшим рейсам.
#     """
#     try:
#         series_data = [{'series': []}]
#
#         class_bron_season_query = session.query(
#             ClassBronSeason.SDAT_S,
#             ClassBronSeason.Increment_day,
#         )
#
#         class_bron_season_query = class_bron_season_query.filter(
#             ClassBronSeason.FLT_NUM == flight_number,
#             ClassBronSeason.SEG_CLASS_CODE == booking_class,
#             ClassBronSeason.SDAT_S.between(booking_start, booking_end)
#         )
#
#         dates_receipt = []
#         increments_days = []
#
#         for result in class_bron_season_query.all():
#             sdat_s = result.SDAT_S
#             increment_day = result.Increment_day
#
#             dates_receipt.append(sdat_s)
#             increments_days.append(increment_day)
#
#         series_data[0]['series'].append({
#             "name": "График спроса",
#             "type": "column",
#             "data": increments_days,
#         })
#
#         season_query = (
#             session
#             .query(distinct(SeasonMosSochi.Season_name))
#             .filter(SeasonMosSochi.date_season.between(booking_start, booking_end))
#         )
#
#         seasons_names = [season_name[0] for season_name in season_query]
#
#         seasons_dates = []
#         for season_name in seasons_names:
#
#             start_date_query = (
#                 session.query(SeasonMosSochi.date_season)
#                 .filter(
#                     SeasonMosSochi.Season_name == season_name,
#                     SeasonMosSochi.Direction == direction
#                 )
#                 .order_by(SeasonMosSochi.date_season.asc())
#                 .limit(1)
#             )
#             start_date = start_date_query.scalar()
#
#             end_date_query = (
#                 session.query(SeasonMosSochi.date_season)
#                 .filter(
#                     SeasonMosSochi.Season_name == season_name,
#                     SeasonMosSochi.Direction == direction
#                 )
#                 .order_by(SeasonMosSochi.date_season.desc())
#                 .limit(1)
#             )
#             end_date = end_date_query.scalar()
#
#             dates_between = []
#             current_date = start_date
#
#             while current_date <= end_date:
#                 dates_between.append(current_date)
#                 current_date += timedelta(days=1)
#
#             seasons_dates.append({"season_name": season_name, "dates": dates_between})
#
#         for el in seasons_dates:
#             name = el['season_name']
#             dates = el['dates']
#
#             heights = []
#             for season_date in dates:
#                 result = (
#                     session
#                     .query(SeasonMosSochi.Height)
#                     .filter(
#                         SeasonMosSochi.Season_name == name,
#                         SeasonMosSochi.date_season == season_date
#                     )
#                     .scalar()
#                 )
#
#                 if result:
#                     heights.append(result)
#                 else:
#                     heights.append(0)
#
#             print(name, heights)
#             series_data[0]['series'].append({
#                 "name": name,
#                 "type": "column",
#                 "data": heights,
#             })
#
#         res_data = process_result_season_data(series_data, dates_receipt)
#
#         return {'status': 200, 'data': res_data}
#
#     except Exception as e:
#         return {'status': 500, 'error': str(e)}


# @router.get("/seasonality")
# async def get_seasonality(
#         direction: str = Query(..., description="Направление рейса", example="Москва - Сочи"),
#         flight_number: str = Query(..., description="Номер рейса", example="1120"),
#         booking_class: str = Query(..., description="Класс бронирования", example="Y"),
#         booking_start: Optional[date] = Query(...,
#                                               description="Период для просмотра стартовая дата",
#                                               example='2018-05-29'),
#         booking_end: Optional[date] = Query(...,
#                                             description="Период для просмотра конечная дата",
#                                             example='2019-12-31')
#
# ):
#     """
#     Определение динамики бронирований рейса в разрезе
#     классов бронирования по вылетевшим рейсам.
#     """
#     try:
#         series_data = [{'series': []}]
#
#         class_bron_season_query = session.query(
#             ClassBronSeason.SDAT_S,
#             ClassBronSeason.Increment_day,
#         )
#
#         class_bron_season_query = class_bron_season_query.filter(
#             ClassBronSeason.FLT_NUM == flight_number,
#             ClassBronSeason.SEG_CLASS_CODE == booking_class,
#             ClassBronSeason.SDAT_S.between(booking_start, booking_end)
#         )
#
#         dates_receipt = []
#         increments_days = []
#
#         for result in class_bron_season_query.all():
#             sdat_s = result.SDAT_S
#             increment_day = result.Increment_day
#
#             dates_receipt.append(sdat_s)
#             increments_days.append(increment_day)
#
#         series_data[0]['series'].append({
#             "name": "График спроса",
#             "type": "column",
#             "data": increments_days,
#         })
#
#         season_query = (
#             session.query(distinct(SeasonMosSochi.Season_name))
#             .filter(SeasonMosSochi.date_season.between(booking_start, booking_end))
#         )
#         seasons_names = [season_name[0] for season_name in season_query]
#
#         for season_name in seasons_names:
#             height = []
#
#             for date_receipt in dates_receipt:
#                 query = session.query(
#                     SeasonMosSochi.Height,
#                 )
#
#                 query = query.filter(
#                     SeasonMosSochi.Season_name == season_name,
#                     SeasonMosSochi.Direction == direction,
#                     SeasonMosSochi.date_season == date_receipt,
#                 )
#
#                 result = query.first()
#                 if result:
#                     data = result.Height
#                 else:
#                     data = 0
#
#                 height.append(data)
#
#             series_data[0]['series'].append({
#                 "name": season_name,
#                 "type": "column",
#                 "data": height,
#             })
#
#         res_data = process_result_season_data(series_data, dates_receipt)
#
#         return {'status': 200, 'data': res_data}
#
#     except Exception as e:
#         return {'status': 500, 'error': str(e)}


# @router.get("/seasonality")
# async def get_seasonality(
#         direction: str = Query(..., description="Направление рейса", example="Москва - Сочи"),
#         flight_number: str = Query(..., description="Номер рейса", example="1120"),
#         booking_class: str = Query(..., description="Класс бронирования", example="Y"),
#         booking_start: Optional[date] = Query(...,
#                                               description="Период для просмотра стартовая дата",
#                                               example='2018-05-29'),
#         booking_end: Optional[date] = Query(...,
#                                             description="Период для просмотра конечная дата",
#                                             example='2019-12-31')
#
# ):
#     """
#     Определение динамики бронирований рейса в разрезе
#     классов бронирования по вылетевшим рейсам.
#     """
#     try:
#
#         series_data = [{'series': []}]
#
#         class_bron_season_query = session.query(
#             ClassBronSeason.SDAT_S,
#             ClassBronSeason.Increment_day,
#         )
#
#         class_bron_season_query = class_bron_season_query.filter(
#             ClassBronSeason.FLT_NUM == flight_number,
#             ClassBronSeason.SEG_CLASS_CODE == booking_class,
#             ClassBronSeason.SDAT_S.between(booking_start, booking_end)
#         )
#
#         dates_receipt = []
#         increments_days = []
#
#         for result in class_bron_season_query.all():
#             sdat_s = result.SDAT_S
#             increment_day = result.Increment_day
#
#             dates_receipt.append(sdat_s)
#             increments_days.append(increment_day)
#
#         series_data[0]['series'].append({
#             "name": "График спроса",
#             "type": "column",
#             "data": increments_days,
#         })
#
#         season_query = (
#             session
#             .query(distinct(SeasonMosSochi.Season_name))
#             .filter(SeasonMosSochi.date_season.between(booking_start, booking_end))
#         )
#         seasons_names = [season_name[0] for season_name in season_query]
#
#         for season_name in seasons_names:
#             query = session.query(
#                 SeasonMosSochi.Season_name,
#                 SeasonMosSochi.date_season,
#                 SeasonMosSochi.Height,
#             )
#
#             query = query.filter(
#                 SeasonMosSochi.Season_name == season_name,
#                 SeasonMosSochi.Direction == direction,
#                 SeasonMosSochi.date_season.between(booking_start, booking_end)
#             )
#
#             print(query.all())
#
#         #     heights = []
#         #     for res in query.all():
#         #         height = res.Height
#         #         data = res.date_season
#         #
#         #         data.append()
#         #
#         #     series_data[0]['series'].append({
#         #         "name": season_name,
#         #         "type": "column",
#         #         "data": heights,
#         #     })
#         #
#         # res_data = process_result_season_data(series_data, dates_receipt)
#         #
#         # return {'status': 200, 'data': res_data}
#         # print(dates_receipt)
#         # season_data = []
#         # for season_name in seasons_names:
#         #     season_query = (
#         #         session
#         #         .query(
#         #             SeasonMosSochi.Season_name,
#         #             SeasonMosSochi.Height,
#         #             SeasonMosSochi.date_season
#         #         ).filter(
#         #             SeasonMosSochi.Season_name == season_name,
#         #             SeasonMosSochi.Direction == direction,
#         #             SeasonMosSochi.date_season.between(booking_start, booking_end)
#         #         ).all()
#         #     )
#         #
#         #     # print(season_query)
#         #     data = []
#         #     for result in season_query:
#         #         dates = result.date_season
#         #         height = result.Height
#         #
#         #         data.append({
#         #             "dates": str(dates),
#         #             "height": int(height),
#         #         })
#         #
#         #     season_data.append({"season_name": season_name, "dates": data})
#         #
#         # print(json.dumps(season_data, indent=2, ensure_ascii=False))
#
#         # height = []
#         # for date_receipt in dates_receipt:
#         #     query = session.query(
#         #         SeasonMosSochi.Height,
#         #     )
#         #
#         #     query = query.filter(
#         #         # SeasonMosSochi.Season_name == season_name,
#         #         SeasonMosSochi.Direction == direction,
#         #         SeasonMosSochi.date_season == date_receipt,
#         #     )
#         #
#         #     if query.all():
#         #         for result in query.all():
#         #             data = result.Height
#         #
#         #             height.append(data)
#         #     else:
#         #         height.append(0)
#         #
#         #     series_data[0]['series'].append({
#         #         # "name": season_name,
#         #         "type": "column",
#         #         "data": height,
#         #     })
#
#         res_data = process_result_season_data(series_data, dates_receipt)
#
#         return {'status': 200, 'data': res_data}
#
#     except Exception as e:
#         return {'status': 500, 'error': str(e)}
#
#     # for season_name in seasons_names:
#     #
#     #     height = []
#     #     for date_receipt in dates_receipt:
#     #         query = session.query(
#     #             SeasonMosSochi.Height,
#     #         )
#     #
#     #         query = query.filter(
#     #             SeasonMosSochi.Season_name == season_name,
#     #             SeasonMosSochi.Direction == direction,
#     #             SeasonMosSochi.date_season == date_receipt,
#     #         )
#     #
#     #         if query.all():
#     #             for result in query.all():
#     #                 data = result.Height
#     #
#     #                 height.append(data)
#     #         else:
#     #             height.append(0)
#     #
#     #     series_data[0]['series'].append({
#     #         "name": season_name,
#     #         "type": "column",
#     #         "data": height,
#     #     })


@router.get("/demand-forecast")
async def get_demand_forecast(
        flight_number: int = Query(..., description="Номер рейса", example="1116"),
        flight_date: str = Query(..., description="Дата рейса", example="2020-03-29"),
        booking_class: str = Query(..., description="Класс бронирования", example="B"),
        booking_period: Optional[int] = Query(1, ge=1, le=12,
                                              description="Период прогнозирования спроса для рейса (в месяцах)",
                                              example='1'),
):
    """
    Прогнозирование спроса в разрезе классов бронирования для продаваемых рейсов.
    """
    try:
        flight_date_obj = datetime.strptime(flight_date, "%Y-%m-%d").date()

        booking_period_start_date = flight_date_obj - timedelta(days=booking_period * 30)
        booking_period_end_date = flight_date_obj
        booking_classes = booking_class.replace(" ", "").split(",")

        query = session.query(
            RaspScoreAll.SDAT_S,
            RaspScoreAll.PASS_BK,
        )

        query = query.filter(
            RaspScoreAll.FLT_NUM == flight_number,
            RaspScoreAll.DD == flight_date,
            RaspScoreAll.SEG_CLASS_CODE.in_(booking_classes),
            RaspScoreAll.SDAT_S.between(booking_period_start_date, booking_period_end_date),
        )

        dates_receipt = []
        for result in query.all():
            sdat_s = result.SDAT_S
            dates_receipt.append(sdat_s)

        series_data = [{'series': []}]

        for booking_class in booking_classes:
            pass_query = query.filter(
                RaspScoreAll.SEG_CLASS_CODE == booking_class,
                RaspScoreAll.DTD >= 0,
            )

            pass_bks = [round(result.PASS_BK, 1) for result in pass_query.all()]

            if pass_bks:
                series_data[0]['series'].append({
                    'name': f'Суммарное бронирование {booking_class}',
                    'type': 'line',
                    'data': pass_bks
                })
            else:
                series_data[0]['series'].append({
                    'name': f'Данных для класс {booking_class} не найдено',
                    'type': 'line',
                    'data': [0, 0, 0, 0, 0, 0, 0, 0]
                })

        res_data = process_result_demand_forecast_data(series_data, dates_receipt)

        return {'status': 200, "data": res_data}

    except Exception as e:
        return {'status': 500, "error": str(e)}


@router.get("/demand-profile")
async def get_demand_profile(
        direction: str = Query(..., description="Направление рейса", example="Москва - Сочи"),
        flight_number: str = Query(..., description="Номер рейса", example="1120"),
        booking_class: str = Query(..., description="Класс бронирования", example="Y"),
        booking_start: Optional[date] = Query(None,
                                              description="Период для просмотра динамики бронирования стартовая дата",
                                              example='2018-05-29'),
        booking_end: Optional[date] = Query(None,
                                            description="Период для просмотра динамики бронирования конечная дата",
                                            example='2019-12-31')
):
    """
    Определение профилей спроса в разрезе классов бронирования, по вылетевшим рейсам.
    """
    pass
