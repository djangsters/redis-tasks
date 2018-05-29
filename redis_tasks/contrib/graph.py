import uuid

from redis_tasks import Queue
from redis_tasks.conf import RedisKey, connection
from redis_tasks.utils import atomic_pipeline, serialize, deserialize


def chain(members):
    graph = TaskGraph()
    tail = None
    for member in members:
        node = graph.add_task(member)
        if tail:
            graph.add_dependency(tail, node)
        tail = node
    return graph


class TaskGraph:
    def __init__(self, id=None):
        self.id = id or str(uuid.uuid4())
        self.key = RedisKey(f"ston.graph:{self.id}")
        self.nodes = []
        self.edges = set()

    def add_task(self, task):
        if not isinstance(task["func"], str):
            task["func"] = '{0}.{1}'.format(task["func"].__module__, task["func"].__name__)
        node = GraphNode(task)
        self.nodes.append(node)
        return node

    def add_dependency(self, before, after):
        self.edges.add((before, after))

    @atomic_pipeline
    def save(self, *, pipeline):
        if self.nodes:
            pipeline.set(self.key, serialize({
                'nodes': {id(node): (node.task, node.task_id) for node in self.nodes},
                'edges': [(id(a), id(b)) for (a, b) in self.edges]
            }), ex=7 * 24 * 60 * 60)
        else:
            pipeline.delete(self.key)

    def reload(self):
        dct = deserialize(connection.get(self.key))
        nodes = {id: GraphNode(*data) for id, data in dct["nodes"].items()}
        self.nodes = list(nodes.values())
        self.edges = {(nodes[a], nodes[b]) for (a, b) in dct["edges"]}

    def lock(self):
        return connection.lock(f"{self.key}:lock", timeout=10)

    def mark_done(self, task_id):
        self.nodes = [n for n in self.nodes if n.task_id != task_id]
        self.edges = {(a, b) for (a, b) in self.edges if a.task_id != task_id}

    @atomic_pipeline
    def enqueue_ready(self, *, pipeline):
        blocked = {b for (a, b) in self.edges}
        # We want to keep the order of the nodes
        ready = [node for node in self.nodes
                 if not node.task_id and node not in blocked]
        for node in ready:
            queue = Queue(node.task.pop('queue', "default"))
            task = queue.enqueue_call(**node.task, pipeline=pipeline)
            node.task = None
            task.meta["ston.graph"] = self.id
            task.save_meta(pipeline=pipeline)
            node.task_id = task.id

    @atomic_pipeline
    def enqueue(self, *, pipeline):
        if not GraphMiddleware.is_installed():
            raise Exception(
                f"Please add {GraphMiddleware.__module__}.{GraphMiddleware.__name__} "
                "to your MIDDLEWARE")
        self.enqueue_ready(pipeline=pipeline)
        self.save(pipeline=pipeline)


class GraphNode:
    def __init__(self, task=None, task_id=None):
        self.task = task
        self.task_id = task_id


class GraphMiddleware:
    def process_outcome(self, task, *exc_info):
        graph_id = task.meta.get("ston.graph")
        if not graph_id:
            return
        graph = TaskGraph(graph_id)
        with graph.lock():
            # TODO: handle missing graph
            graph.reload()
            graph.mark_done(task.id)
            with connection.pipeline() as pipe:
                graph.enqueue_ready(pipeline=pipe)
                graph.save(pipeline=pipe)
                pipe.execute()

    @classmethod
    def is_installed(cls):
        from redis_tasks.conf import task_middleware
        print(task_middleware)
        return any(issubclass(x, cls) for x in task_middleware)
