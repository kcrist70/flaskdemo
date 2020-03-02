from flask import jsonify,current_app
from . import web
import redis
import time


@web.route('/realtime/<string:robot>', methods=['GET'])
def query(robot):
    redis_con = redis.Redis(host=current_app.config["IP"], port=current_app.config["PORT"], password=current_app.config["PASSWORD"],decode_responses=True)
    mon =  time.strftime("%Y-%m", time.localtime())
    day = time.strftime("%Y-%m-%d", time.localtime())
    with redis_con.pipeline() as p:
        p.hget(robot, robot + "_pv_" + mon).hget(robot, robot + "_pv_" + day).hget(robot, robot + "_uv_" + mon)
        val_list = p.execute()
    for index, value in enumerate(val_list):
        if value is None:
            val_list[index] = 0
    return jsonify(mon_pv=val_list[0],day_pv=val_list[1],mon_uv = val_list[2])

@web.route('/realtime/en/<string:robot>', methods=['GET'])
def query(robot):
    redis_con = redis.Redis(host=current_app.config["IP"], port=current_app.config["PORT"], password=current_app.config["PASSWORD"],decode_responses=True)
    mon =  time.strftime("%Y-%m", time.localtime())
    day = time.strftime("%Y-%m-%d", time.localtime())
    with redis_con.pipeline() as p:
        p.hget(robot, robot + "_en_pv_" + mon).hget(robot, robot + "_en_pv_" + day).hget(robot, robot + "_en_uv_" + mon)
        val_list = p.execute()
    for index, value in enumerate(val_list):
        if value is None:
            val_list[index] = 0
    return jsonify(mon_pv=val_list[0],day_pv=val_list[1],mon_uv = val_list[2])

