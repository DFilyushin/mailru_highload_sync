from flask import request
import json
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from app.utils import timestamp_by_age, raise_
from app import fapp, db
from app.models import Users, Locations, Visits, AlchemyEncoder, DecimalEncoder


entities = {'users': Users, 'visits': Visits, 'locations': Locations}


visit_params = {
        'fromDate':
        {
            'field': 'visited_at',
            'sign': '>',
            'quote': '',
            'param_function': int
        },
        'toDate':
        {
            'field': 'visited_at',
            'sign': '<',
            'quote': '',
            'param_function': int
        },

        'toDistance':
        {
            'field': 'distance',
            'sign': '<',
            'quote': '',
            'param_function': int
        },

        'country':
        {
            'field': 'country',
            'sign': '=',
            'quote': '\'',
            'param_function': None
        }
}

loc_avg = {
    'fromDate':
        {
            'field': 'visited_at',
            'sign': '>',
            'quote': '',
            'param_function': int
        },
    'toDate':
        {
            'field': 'visited_at',
            'sign': '<',
            'quote': '',
            'param_function': int
        },
    'fromAge':
        {
            'field': 'birth_date',
            'sign': '<',
            'quote': '',
            'param_function': timestamp_by_age
        },
    'toAge':
        {
            'field': 'birth_date',
            'sign': '>',
            'quote': '',
            'param_function': timestamp_by_age
        },
    'gender':
        {
            'field': 'gender',
            'sign': '=',
            'quote': '\'',
            'param_function': lambda x: x if len(x)==1 else raise_(ValueError)
        }
}


@fapp.route('/<entity>/<id>', methods=['GET'])
def get_entity(entity, id):
    """
    Get entity item by id
    :param entity:
    :param id:
    :return:
    """
    try:
        id_entity = int(id)
        db_entity = entities[entity]
    except (TypeError, ValueError, IndexError):
        return '', 404
    item = db_entity.query.filter_by(id=id_entity).scalar()
    if not item:
        return '',404
    return json.dumps(item, cls=AlchemyEncoder)


@fapp.route('/users/<int:user_id>/visits', methods=['GET'])
def get_visits(user_id):
    params = request.args
    where_clause = []
    for param in params:
        if params[param] == '':
            return '', 400
        v_param = visit_params.get(param)
        if v_param['param_function']:
            f = v_param['param_function']
            try:
                param_value = f(params[param])
            except:
                return '',400
        else:
            param_value = params[param]
        where_line = "and {} {} {}{}{}".format(v_param['field'], v_param['sign'], v_param['quote'], param_value, v_param['quote'])
        where_clause.append(where_line)

    if not Users.query.filter_by(id=user_id).scalar():
        return '', 404
    where_str = ' '.join(where_clause)
    sql_text = 'select mark, visited_at, place from visits inner join Locations on Locations.id = visits.location_id ' \
               'where visits.user_id={} {} order by visited_at'.format(user_id, where_str)
    result = db.engine.execute(text(sql_text))

    d, a = {}, []
    for row in result:
        for tup in row.items():
            d = {**d, **{tup[0]: tup[1]}}
        a.append(d)
    return json.dumps({'visits': a})


@fapp.route('/locations/<location_id>/avg', methods=['GET'])
def get_location_avg(location_id):
    if not Locations.query.filter_by(id=location_id).scalar():
        return '', 404

    params = request.args
    where_clause = []
    # ignore empty params
    for key, value in params.items():
        if value == '':
            return '', 400
        l_param = loc_avg.get(key)
        if l_param['param_function']:
            f = l_param['param_function']
            try:
                value = f(value)
            except:
                return '', 400
        where_line = "and {} {} {}{}{}".format(l_param['field'], l_param['sign'], l_param['quote'], value, l_param['quote'])
        where_clause.append(where_line)

    where_str = ' '.join(where_clause)

    sql_text = 'select avg(mark) from visits inner join users on visits.user_id = users.id ' \
               'where location_id = {} {}'.format(location_id, where_str)
    sql = text(sql_text)
    avg_value = db.engine.execute(sql).scalar()
    if not avg_value:
        avg_value = 0.0
    return json.dumps({'avg': round(avg_value, 5)}, cls=DecimalEncoder)


@fapp.route('/<entity>/<int:id>', methods=['POST'])
def update_entity(entity, id):
    if (not request.is_json) or (request.is_json and not request.data):
        return '', 400
    try:
        id_entity = int(id)
        db_entity = entities[entity]
    except (TypeError, ValueError, IndexError):
        return '', 400
    entity_item = db_entity.query.filter_by(id=id_entity).scalar()
    if not entity_item:
        return '', 404
    json_post = request.json
    for x in json_post:
        if not json_post[x]:
            return '', 400
    try:
        for key, value in json_post.items():
            setattr(entity_item, key, value)
        db.session.commit()
        return '', 200
    except SQLAlchemyError as e:
        return '', 400


@fapp.route('/<entity>/new', methods=['POST'])
def new_entity(entity):
    json_post = request.json
    try:
        id_entity = int(json_post['id'])
        db_entity = entities[entity]
    except (TypeError, ValueError, IndexError):
        return '', 400
    if db_entity.query.filter_by(id=id_entity).scalar():
        return '', 400

    new_record = db_entity(**json_post)
    try:
        db.session.add(new_record)
        db.session.commit()
        return '', 200
    except SQLAlchemyError as e:
        return '',400
