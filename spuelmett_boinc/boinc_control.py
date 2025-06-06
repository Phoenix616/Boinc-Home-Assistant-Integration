"""Boinc Control integration logic."""

from __future__ import annotations

from datetime import timedelta

from .pyboinc.pyboinc import init_rpc_client
from .pyboinc.pyboinc.rpc_client import Mode


class BoincControl:
    def __init__(self, host: str, password: str, checkpoint_time: int) -> None:
        self.current_soft_stop_state = False
        self.host = host
        self.password = password
        self.checkpoint_time = checkpoint_time
        self.rpc_client = None

    async def connect(self):
        self.rpc_client = await init_rpc_client(self.host, self.password)
        await self.rpc_client.authorize()

    def set_checkpoint_time(self, checkpoint_time):
        self.checkpoint_time = checkpoint_time

    @property
    def soft_stop_state(self):
        return self.current_soft_stop_state

    async def start_boinc(self):
        await self.connect()
        self.current_soft_stop_state = False
        await self.allow_more_project_work()
        await self.resume_all_task()

    async def stop_boinc(self):
        await self.connect()
        await self.no_more_project_work()
        await self.abort_all_non_started_task()
        await self.suspend_all_task()

    async def soft_stop_boinc(self):
        await self.connect()
        await self.no_more_project_work()
        await self.abort_all_non_started_task()
        self.current_soft_stop_state = True

    async def update(self):
        # Do nothing if it should not stop
        if self.current_soft_stop_state is False:
            return

        await self.connect()

        results = await self.rpc_client.get_results()
        for result in results:
            # check if the result is an active task
            if "active_task" in result:
                active_task = result["active_task"]
                checkpoint_cpu_time = active_task["checkpoint_cpu_time"]
                current_cpu_time = active_task["current_cpu_time"]
                project_url = result["project_url"]
                name = result["name"]

                # Check if last checkpoint was longer ago than the configured value
                if current_cpu_time - checkpoint_cpu_time < timedelta(
                    seconds=self.checkpoint_time
                ):
                    # Suspend task
                    await self.rpc_client.suspend_result(project_url, name)

    async def resume_all_task(self):
        results = await self.rpc_client.get_results()

        # if no task are there, results is a string
        if results == "\n":
            return

        for result in results:
            project_url = result["project_url"]
            name = result["name"]
            await self.rpc_client.resume_result(project_url, name)

    async def abort_all_non_started_task(self):
        results = await self.rpc_client.get_results()

        # if no task are there, results is a string
        if results == "\n":
            return

        for result in results:
            if "active_task" not in result or result["active_task"]["scheduler_state"] is "uninitialized":
                project_url = result["project_url"]
                name = result["name"]
                await self.rpc_client.abort_result(project_url, name)

    async def suspend_all_task(self):
        results = await self.rpc_client.get_results()

        # if no task are there, results is a string
        if results == "\n":
            return

        for result in results:
            project_url = result["project_url"]
            name = result["name"]
            await self.rpc_client.suspend_result(project_url, name)

    async def no_more_project_work(self):
        await self.connect()
        projects = await self.rpc_client.get_project_status()

        # if no projects are there, results is a string
        if projects == "\n":
            return

        for project in projects:
            project_url = project.master_url
            await self.rpc_client.project_nomorework(project_url)

    async def allow_more_project_work(self):
        await self.connect()
        projects = await self.rpc_client.get_project_status()

        # if no projects are there, results is a string
        if projects == "\n":
            return

        for project in projects:
            project_url = project.master_url
            await self.rpc_client.project_allowmorework(project_url)
