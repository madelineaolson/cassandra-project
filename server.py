import station_pb2_grpc, station_pb2
import grpc
from concurrent import futures
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
from datetime import datetime
from cassandra.cqltypes import SimpleDateType
from cassandra.cluster import NoHostAvailable
from cassandra import Unavailable 

class StationServicer(station_pb2_grpc.StationServicer):
    
    def __init__(self):
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.session = self.cluster.connect('weather')

        self.insert_statement = self.session.prepare("INSERT INTO stations (id, date, record) VALUES (?, ?, {tmin:?, tmax:?})")

        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        
        self.max_statement = self.session.prepare("SELECT MAX(record.tmax) FROM stations WHERE id = ?")

        self.max_statement.consistency_level = ConsistencyLevel.THREE

    def RecordTemps(self, request, context):
        try:
            
            formatted_date = datetime.strptime(request.date, '%Y%m%d').strftime('%Y-%m-%d')
            tmin = int(request.tmin)
            tmax = int(request.tmax)

            print(self.insert_statement, (request.station, formatted_date, tmin, tmax))
            self.session.execute(self.insert_statement, (request.station, formatted_date, tmin, tmax))
            return station_pb2.RecordTempsReply()
       
        except Unavailable as e:
            error_msg = f'need {e.required_replicas} replicas, but only have {e.alive_replicas}'
            return station_pb2.RecordTempsReply(error=error_msg)
       
        except NoHostAvailable as e:
            for error_key, error_value in e.errors.items():
                if isinstance(error_value, Unavailable):
                    error_msg = f'need {error_value.required_replicas} replicas, but only have {error_value.alive_replicas}'
                    return station_pb2.RecordTempsReply(error=error_msg)
            return station_pb2.RecordTempsReply(error='No host available for Cassandra connection')
        
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

    
    def StationMax(self, request, context):
        try:
            result = self.session.execute(
                self.max_statement, (request.station,))
            max_tmax = result[0][0] if result else 0

            return station_pb2.StationMaxReply(tmax=max_tmax)
       
        except Unavailable as e:
            error_msg = f'need {e.required_replicas} replicas, but only have {e.alive_replicas}'
            return station_pb2.StationMaxReply(error=error_msg)
        
        except NoHostAvailable as e:
            for error_key, error_value in e.errors.items():
                if isinstance(error_value, Unavailable):
                    error_msg = f'need {error_value.required_replicas} replicas, but only have {error_value.alive_replicas}'
                    return station_pb2.StationMaxReply(error=error_msg)
            return station_pb2.StationMaxReply(error='No host available for Cassandra connection')
        
        except Exception as e:
            return station_pb2.StationMaxReply(error=str(e))
            

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))

    station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)

    server.add_insecure_port("[::]:5440")

    server.start()

    server.wait_for_termination()

if __name__ == "__main__":
    server()


