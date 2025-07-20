import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy  # For QoS settings
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup

from geometry_msgs.msg import Vector3
from std_msgs.msg import Float64, Bool
from std_srvs.srv import Trigger
# Import custom service definitions
from ros2_library_interfaces.srv import Float32cust, Vector3cust
from ros2_library_interfaces.msg import MovementExchange

import math
import sys
import threading
import time
import copy


#for collision detection
MAX_ATTEMPTS = 3           # number of attempts for retry to execute command after collision detection 
WAIT_INTERVAL = 4.0        # waiting time for retrying
RESPONSE_TIMEOUT = 0.5     # waiting time for responses
# minimum allowed separation distance in meters between path
MIN_DISTANCE = 3.0

def geo_to_enu(p: Vector3, origin: Vector3):
    """
    Converts (lat, lon, alt) WGS84 in (East, North, Up) in meters, 
    based on the origin point origin.
    """
    R = 6378137.0
    # angular differences in radians
    dlat = math.radians(p.x - origin.x)
    dlon = math.radians(p.y - origin.y)
    north = R * dlat
    east  = R * math.cos(math.radians(origin.x)) * dlon
    up    = p.z - origin.z
    return east, north, up

def segments_intersect(a1: Vector3, a2: Vector3,
                       b1: Vector3, b2: Vector3) -> bool:
    """
    True if 3D segments a1->a2 and b1->b2 pass within MIN_DISTANCE meters.
    a1,a2,b1,b2 are Vector3 with x=lat, y=lon, z=alt.
    """
    # 1) it projects the four points in ENU (meters), using a1 as the origin
    A1 = (0.0, 0.0, 0.0)
    A2 = geo_to_enu(a2, a1)
    B1 = geo_to_enu(b1, a1)
    B2 = geo_to_enu(b2, a1)
    # 2) vector calculate
    ux, uy, uz = (A2[i] - A1[i] for i in range(3))
    vx, vy, vz = (B2[i] - B1[i] for i in range(3))
    wx, wy, wz = (A1[i] - B1[i] for i in range(3))
    uu = ux*ux + uy*uy + uz*uz
    vv = vx*vx + vy*vy + vz*vz
    uv = ux*vx + uy*vy + uz*vz
    wu = wx*ux + wy*uy + wz*uz
    wv = wx*vx + wy*vy + wz*vz
    # 3) special cases: points
    if uu == 0 and vv == 0:
        return (wx*wx + wy*wy + wz*wz) <= MIN_DISTANCE**2
    if uu == 0 or vv == 0:
        # segment is a point
        if uu == 0:
            px, py, pz = A1
            q1x, q1y, q1z = B1
            dx, dy, dz = vx, vy, vz
            qq = vv or 1.0
        else:
            px, py, pz = B1
            q1x, q1y, q1z = A1
            dx, dy, dz = ux, uy, uz
            qq = uu or 1.0
        t = ((px - q1x)*dx + (py - q1y)*dy + (pz - q1z)*dz) / qq
        t = max(0.0, min(1.0, t))
        qx = q1x + dx*t
        qy = q1y + dy*t
        qz = q1z + dz*t
        return ((px - qx)**2 + (py - qy)**2 + (pz - qz)**2) <= MIN_DISTANCE**2
    # 4) general case
    denom = uu*vv - uv*uv
    if denom == 0:
        # parallel segments: endpoint-segment distance control
        def dist2_pt_seg(px, py, pz, q1, dx, dy, dz, qq):
            t = ((px - q1[0])*dx + (py - q1[1])*dy + (pz - q1[2])*dz) / (qq or 1.0)
            t = max(0.0, min(1.0, t))
            qx = q1[0] + dx*t
            qy = q1[1] + dy*t
            qz = q1[2] + dz*t
            return (px - qx)**2 + (py - qy)**2 + (pz - qz)**2
        d2_list = [
            dist2_pt_seg(*A1, B1, vx, vy, vz, vv),
            dist2_pt_seg(*A2, B1, vx, vy, vz, vv),
            dist2_pt_seg(*B1, A1, ux, uy, uz, uu),
            dist2_pt_seg(*B2, A1, ux, uy, uz, uu),
        ]
        return min(d2_list) <= MIN_DISTANCE**2
    # 5) parameters s, t for the minimum internal distance
    s = (uv*wv - vv*wu) / denom
    t = (uu*wv - uv*wu) / denom
    s = max(0.0, min(1.0, s))
    t = max(0.0, min(1.0, t))
    # 6) closest points
    cx = A1[0] + ux*s
    cy = A1[1] + uy*s
    cz = A1[2] + uz*s
    dxp = B1[0] + vx*t
    dyp = B1[1] + vy*t
    dzp = B1[2] + vz*t
    # 7) threshold control
    return ((cx - dxp)**2 + (cy - dyp)**2 + (cz - dzp)**2) <= MIN_DISTANCE**2


class CollisionAvoidanceNode(Node):
    def __init__(self, node_name, namespace):
        super().__init__(node_name, namespace=namespace)
        self._lock = threading.Lock()  # lock for shared state
        self.drone_id = namespace
        namespace = '/' + namespace + '/movement/'
        self.num_transaction = 0 #id of request
        self.priority = 0 
        """[0..MAX_ATTEMPTS-1] every attempt increase priority
        (drone_id, num_transaction, priority) IS PRIMARY KEY OF REQUEST
        we use priority also like counter to not go over MAX_ATTEMPTS attempts"""
        self.num_responses_expected = None #number of collision_avoidance nodes in the system - 1 (me) 
        self.no_collision = None #boolean after all collision responses/attempts
        #home of copter
        self.home_pos = Vector3()
        #current destination 
        self.destination = Vector3()
        #destination of the current request
        self.request_destination = Vector3()
        #collision event for waiting official boolean response at command request
        self._collision_event = threading.Event()
        #timer for do another attempt
        self.retry_timer = None
        #timer for waiting all responses
        self.responses_timer = None
        #
        self.waiting_end_command_event = threading.Event()

        self.timer_RESPONSE_TIMEOUT_active = threading.Event()
        self.timer_RESPONSE_TIMEOUT_active.clear() #flag False 

        self.request_in_progress = threading.Event()
        self.request_in_progress.clear() #flag False 


        # callback groups for three threads
        self.movement_collision_group = MutuallyExclusiveCallbackGroup() # general topics and services for collision movement
        self.movement_library_group = MutuallyExclusiveCallbackGroup()   # general topics and services for library movement
        self.position_library = MutuallyExclusiveCallbackGroup()         # library position
        self.end_command_library = MutuallyExclusiveCallbackGroup()      # end command for topics
        self.publisher_group = MutuallyExclusiveCallbackGroup()          # publisher for the request
        self.subscriber_group = MutuallyExclusiveCallbackGroup()         # subscriber 
        self.timer_WAIT_INTERVAL = MutuallyExclusiveCallbackGroup()      # timer for retrying another attempt
        self.timer_RESPONSE_TIMEOUT = MutuallyExclusiveCallbackGroup()   # timer for waiting all responses

        # collision exchanges
        self.collision_publisher = self.create_publisher(MovementExchange, '/collision_exchange', 10, callback_group=self.publisher_group)
        self.collision_subscriber = self.create_subscription(MovementExchange, '/collision_exchange', self.on_exchange_callback, 10, callback_group=self.subscriber_group)
        

        #COLLISION CHANNELS
        # Service for takeoff (custom Float32 service)
        self.takeoff_server = self.create_service(Float32cust, namespace + 'takeoff_command_collision', self.takeoff_callback, callback_group=self.movement_collision_group)   
        # Service for landing (standard Trigger service)
        self.land_server = self.create_service(Trigger, namespace + 'land_command_collision', self.land_callback, callback_group=self.movement_collision_group)        
        # Service for RTH (Return to Home and stay hovering) (standard Trigger service)
        self.rth_server = self.create_service(Trigger, namespace + 'rth_command_collision', self.rth_callback, callback_group=self.movement_collision_group)
        # Service for RTL (Return to Launch, return to home + land + disarm) (standard Trigger service)
        self.rtl_server = self.create_service(Trigger, namespace + 'rtl_command_collision', self.rtl_callback, callback_group=self.movement_collision_group)
        # Subscribers using default (Reliable) QoS
        # Create a subscriber for the "goto_command_collision" topic
        self.goto_subscriber = self.create_subscription(Vector3, namespace + 'goto_command_collision', self.goto_callback, 10, callback_group=self.movement_collision_group)           
        # Create two subscribers for move commands (topics)
        self.movexyz_subscriber = self.create_subscription(Vector3, namespace + 'movexyz_command_collision', self.movexyz_callback, 10, callback_group=self.movement_collision_group)
        self.moveNED_subscriber = self.create_subscription(Vector3, namespace + 'moveNED_command_collision', self.moveNED_callback, 10, callback_group=self.movement_collision_group)    

        #LIBRARY CHANNELS
        self.takeoff_client = self.create_client(Float32cust, namespace + 'takeoff_command', callback_group=self.movement_library_group)
        self.land_client = self.create_client(Trigger, namespace + 'land_command', callback_group=self.movement_library_group)
        self.rth_client = self.create_client(Trigger, namespace + 'rth_command', callback_group=self.movement_library_group)
        self.rtl_client = self.create_client(Trigger, namespace + 'rtl_command', callback_group=self.movement_library_group)
        self.home_pos_client = self.create_client(Vector3cust, namespace + 'home_position', callback_group=self.movement_library_group)
        self.goto_publisher = self.create_publisher(Vector3, namespace + 'goto_command', 10, callback_group=self.movement_library_group)
        self.movexyz_publisher = self.create_publisher(Vector3, namespace + 'movexyz_command', 10, callback_group=self.movement_library_group)
        self.moveNED_publisher = self.create_publisher(Vector3, namespace + 'moveNED_command', 10, callback_group=self.movement_library_group)
        # Subscriber for take position of copter
        position_qos = QoSProfile(
            depth=10,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST
        )
        self.position_subscriber = self.create_subscription(Vector3, namespace + 'get_position', self.position_callback, position_qos, callback_group=self.position_library)
        self.position = Vector3()

        self.end_command_subscriber = self.create_subscription(Bool, namespace + 'end_command', self.end_command_callback, 10, callback_group=self.end_command_library)    


    def get_home_pos(self):    
        while not self.home_pos_client.wait_for_service(timeout_sec=1.0):
            pass #waiting the activation of service (run library_node)
        request = Vector3cust.Request()    
        future = self.home_pos_client.call(request)
        if future.success is not None:
            resp = future.success
            self.home_pos.x = resp.x
            self.home_pos.y = resp.y
            self.home_pos.z = resp.z

    def takeoff_callback(self, request, response):
        altitude = request.data
        with self._lock:
            goal = Vector3(x=self.position.x, y=self.position.y, z=altitude + self.position.z)
        if self._request_collision(goal):
            # Ensure the takeoff service is available
            if not self.takeoff_client.wait_for_service(timeout_sec=2.0):
                response.success = False
                response.message = 'Takeoff service not available'
                return response
            with self._lock:
                #update node's current path
                self.destination = goal
            # Forward the request to the LibraryNode
            future = self.takeoff_client.call(request)
            with self._lock:
                #update node's current path after executing command (also if failed)
                self.destination = self.position

            # Return the LibraryNode's response verbatim
            return future
        else:
            response.success = False
            response.message = 'Takeoff blocked: potential collision detected'
            return response

    def land_callback(self, request, response):
        with self._lock:
            goal = Vector3(x=self.position.x, y=self.position.y, z=0)
        if self._request_collision(goal):
            # Ensure the landing service is available
            if not self.land_client.wait_for_service(timeout_sec=2.0):
                response.success = False
                response.message = 'Land service not available'
                return response
            with self._lock:
                #update node's current path
                self.destination = goal
            # Forward the request to the LibraryNode
            future = self.land_client.call(request)
            with self._lock:
                #update node's current path after executing command (also if failed)
                self.destination = self.position

            # Return the LibraryNode's response verbatim
            return future
        else:
            response.success = False
            response.message = 'Land blocked: potential collision detected'
            return response

    def rth_callback(self, request, response):
        self.get_home_pos()
        goal = self.home_pos
        if self._request_collision(goal):
            # Ensure the rth service is available
            if not self.rth_client.wait_for_service(timeout_sec=2.0):
                response.success = False
                response.message = 'rth service not available'
                return response
            with self._lock:
                #update node's current path
                self.destination = goal
            # Forward the request to the LibraryNode
            future = self.rth_client.call(request)
            with self._lock:
                #update node's current path after executing command (also if failed)
                self.destination = self.position

            # Return the LibraryNode's response verbatim
            return future
        else:
            response.success = False
            response.message = 'rth blocked: potential collision detected'
            return response

    def rtl_callback(self, request, response):
        self.get_home_pos()
        goal = self.home_pos
        if self._request_collision(goal):
            # Ensure the rtl service is available
            if not self.rtl_client.wait_for_service(timeout_sec=2.0):
                response.success = False
                response.message = 'rtl service not available'
                return response
            with self._lock:
                #update node's current path
                self.destination = goal
            # Forward the request to the LibraryNode
            future = self.rtl_client.call(request)
            with self._lock:
                #update node's current path after executing command (also if failed)
                self.destination = self.position

            # Return the LibraryNode's response verbatim
            return future
        else:
            response.success = False
            response.message = 'rtl blocked: potential collision detected'
            return response

    def goto_callback(self, msg):
        goal = Vector3(x=msg.x, y=msg.y, z=msg.z)
        if self._request_collision(goal):
            with self._lock:
                #update node's current path
                self.destination = goal
            # forward to the normal LibraryNode topic
            self.waiting_end_command_event.clear()
            self.goto_publisher.publish(msg)
            # to prevent topic callbacks from being executed immediately, so the collision algorithm evaluates commands sequentially
            self.waiting_end_command_event.wait() #waiting for the end library command
            self.waiting_end_command_event.clear()

        else:
            self.get_logger().warn('Goto blocked: potential collision detected')

    def movexyz_callback(self, msg):
        with self._lock:
            goal = self.local_offset_to_geo_NED(self.position, msg) #with copter's yaw-->north 
        if self._request_collision(goal):
            with self._lock:
                #update node's current path
                self.destination = goal
            self.waiting_end_command_event.clear()
            self.movexyz_publisher.publish(msg)
            # to prevent topic callbacks from being executed immediately, so the collision algorithm evaluates commands sequentially
            self.waiting_end_command_event.wait() #waiting for the end library command
            self.waiting_end_command_event.clear() 
        else:
            self.get_logger().warn('Move XYZ blocked: potential collision detected')       

    def moveNED_callback(self, msg):
        with self._lock:
            goal = self.local_offset_to_geo_NED(self.position, msg) #approximately
        if self._request_collision(goal):
            with self._lock:
                #update node's current path
                self.destination = goal
            self.waiting_end_command_event.clear()
            self.moveNED_publisher.publish(msg)
            # to prevent topic callbacks from being executed immediately, so the collision algorithm evaluates commands sequentially
            self.waiting_end_command_event.wait() #waiting for the end library command
            self.waiting_end_command_event.clear()
        else:
            self.get_logger().warn('Move NED blocked: potential collision detected')

    def end_command_callback(self, msg):
        with self._lock:
            self.destination = self.position
            self.waiting_end_command_event.set()

    def position_callback(self, msg):
        with self._lock:
            self.position = msg
            if (self.destination.x == 0.0 and self.destination.y == 0.0 and self.destination.z == 0.0):
                self.destination = self.position


    #callback when arrives a new request
    #ignore if a request of mine is published or a response to a request that is not mine
    def on_exchange_callback(self, msg: MovementExchange):
        with self._lock:
            #I am during a request               and arrive request and by another node                    and other request's path collide with mine
            if self.request_in_progress.is_set() and msg.is_request and msg.requestor_id != self.drone_id and segments_intersect(self.position, self.request_destination, msg.start, msg.goal): 
                #i haven't the priority (nr attempts)   or egual priority but more transaction done                                            or the priority and number of transactions are the same but my droneId is minor
                if (self.priority < msg.priority_value) or (self.priority == msg.priority_value and self.num_transaction > msg.transaction_id) or (self.priority == msg.priority_value and self.num_transaction == msg.transaction_id and self.drone_id < msg.requestor_id):
                    #delete my request
                    #publish response if collision or not
                    new_msg: MovementExchange = copy.deepcopy(msg)
                    new_msg.can_move = not (segments_intersect(self.position, self.destination, new_msg.start, new_msg.goal))
                    new_msg.is_request = False
                    self.collision_publisher.publish(new_msg)
                    if self.timer_RESPONSE_TIMEOUT_active.is_set(): #if i am waiting responses, delete timer a try another attemp
                        self.responses_timer.cancel() #cancel the waiting_responses timer, i have to delete my request
                        self.timer_RESPONSE_TIMEOUT_active.clear() 
                        if self.priority + 1 < MAX_ATTEMPTS: #I can do another try 
                            self.priority += 1
                            new_msg.priority_value = self.priority
                            new_msg.is_request = True
                            self.waiting_interval(new_msg) # wait WAIT_INTERVAL seconds to make another attempt
                        else:
                            #finish NUM_ATTEMPTS attempts
                            self.no_collision = False
                            self._collision_event.set()
                    #else: timer deactivate, when it expired it tried the another attempt
                #else: i can move on with my request because i have the absolute priority

            else:
                if msg.is_request: #REQUEST
                    if msg.requestor_id != self.drone_id: #OTHER REQUEST (NOT MINE)
                        #publish response if collision or not
                        new_msg: MovementExchange = copy.deepcopy(msg)
                        new_msg.can_move = not (segments_intersect(self.position, self.destination, new_msg.start, new_msg.goal))
                        new_msg.is_request = False
                        self.collision_publisher.publish(new_msg)
                else: #RESPONSE: has is_request=False
                    #RESPONSE TO MY REQUEST
                    #unique drone identifier, transaction and priority AND the timer for waiting responses is active
                    if msg.requestor_id == self.drone_id and msg.transaction_id == self.num_transaction and msg.priority_value == self.priority and self.timer_RESPONSE_TIMEOUT_active.is_set():
                        if not msg.can_move: #collision detected, movement not permitted
                            self.responses_timer.cancel()
                            self.timer_RESPONSE_TIMEOUT_active.clear()
                            if self.priority + 1 < MAX_ATTEMPTS: #I can do another try 
                                self.priority += 1
                                new_msg: MovementExchange = copy.deepcopy(msg)
                                new_msg.priority_value = self.priority
                                new_msg.is_request = True
                                self.waiting_interval(new_msg) # wait WAIT_INTERVAL seconds to make another attempt
                            else:
                                #finish NUM_ATTEMPTS attempts
                                self.no_collision = False
                                self._collision_event.set()
                        else: 
                            self.num_responses_expected -= 1 #recieved a True response
                            if self.num_responses_expected == 0: #I recieved all the responses by other nodes
                                self.responses_timer.cancel() #cancel the waiting_responses timer, i have all responses
                                self.timer_RESPONSE_TIMEOUT_active.clear()
                                self.no_collision = True
                                self._collision_event.set()

    #collision avoidance: send the request to other collision node on collision topic
    #return False if collision detected, block the command, else True
    def _request_collision(self, goal):
        with self._lock:
            self.num_transaction += 1
            self.priority = 0 #minimum priority (first try)
            self.no_collision = None
            self.request_destination = goal
            self.num_responses_expected = self.collision_publisher.get_subscription_count() - 1
        if self.num_responses_expected <= 0: #no collision detected, there is only one copter
            self.no_collision = True
            return True
        self.request_in_progress.set() #during a request.. 
        req = MovementExchange()
        req.requestor_id = self.drone_id
        with self._lock: 
            req.transaction_id = self.num_transaction
            req.priority_value = self.priority
            req.start = self.position
        req.goal = goal
        req.is_request = True
        self._collision_event.clear()
        self.collision_publisher.publish(req) #publish the request on the channel
        self.waiting_response_timeout(req) #start timeout for waiting responses
        # wait for response under lock
        self._collision_event.wait()   # suspends the thread until .set() arrives
        self.request_in_progress.clear()
        with self._lock:
            return self.no_collision

    #waiting for another attempts
    def waiting_interval(self, msg: MovementExchange):
        # inner callback: finish waiting, publishes msg and deletes timer
        def on_retry_timer():
            self.retry_timer.cancel()
            self.collision_publisher.publish(msg)
            self.waiting_response_timeout(msg) #start timeout for waiting responses
        # create one‐shot timer in timer_WAIT_INTERVAL thread
        self.retry_timer = self.create_timer(WAIT_INTERVAL, on_retry_timer, callback_group=self.timer_WAIT_INTERVAL)

    #waiting responses timeout    
    def waiting_response_timeout(self, msg: MovementExchange):           
        # inner callback: finish timer waiting all responses 
        def waiting_responses_timer():
            #cancel the timer and deactivate the flag
            self.responses_timer.cancel()
            self.timer_RESPONSE_TIMEOUT_active.clear()
            with self._lock:
                #timeout expired: control to do another attempt
                if self.priority + 1 < MAX_ATTEMPTS: #I can do another try 
                    self.priority += 1
                    new_msg: MovementExchange = copy.deepcopy(msg)
                    new_msg.priority_value = self.priority
                    new_msg.is_request = True
                    self.waiting_interval(new_msg) # wait WAIT_INTERVAL seconds to make another attempt
                else:
                    #finish NUM_ATTEMPTS attempts
                    self.no_collision = False
                    self._collision_event.set()    
        self.timer_RESPONSE_TIMEOUT_active.set()
        # create one‐shot timer in timer_RESPONSE_TIMEOUT thread
        self.responses_timer = self.create_timer(RESPONSE_TIMEOUT, waiting_responses_timer,callback_group=self.timer_RESPONSE_TIMEOUT)


    #calculate the coordinates by a point of start and a movement in 3D (in meters)
    def local_offset_to_geo_NED(self, position: Vector3, offset: Vector3) -> Vector3:
        #GEOGRAPHIC NORTH
        # mean Earth radius in meters (WGS84)
        R = 6378137.0
        # unpack
        lat = position.x
        lon = position.y
        alt = position.z
        dn = offset.x
        de = offset.y
        du = offset.z
        # Δφ = dNorth / R
        delta_lat = (dn / R) * (180.0 / math.pi)
        # Δλ = dEast / (R * cos φ)
        # use the *current* latitude for the cosine
        delta_lon = (de / (R * math.cos(math.radians(lat)))) * (180.0 / math.pi)
        new_lat = lat + delta_lat
        new_lon = lon + delta_lon
        new_alt = alt + du
        return Vector3(x=new_lat, y=new_lon, z=new_alt)


def main(args=None):
    if len(sys.argv) > 1: #namespace from CLI (ex 'copter1')
        namespace = sys.argv[1] 
        node_name = 'collision_avoidance'
        rclpy.init(args=args)
        node = CollisionAvoidanceNode(node_name, namespace)
        from rclpy.executors import MultiThreadedExecutor
        executor = MultiThreadedExecutor(num_threads=8)
        executor.add_node(node)
        executor.spin()
        node.destroy_node()
        rclpy.shutdown()

    else:
        print("Execute: ros2 run ros2_library 'node' 'namespace_copter'")

if __name__ == '__main__':
    main()

