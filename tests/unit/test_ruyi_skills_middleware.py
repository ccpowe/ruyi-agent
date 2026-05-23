from __future__ import annotations

from deepagents.backends.protocol import FileDownloadResponse, LsResult

from ruyi_agent.runtime.middleware.ruyi_skills import RuyiSkillsMiddleware


class BackendView:
    def ls(self, path: str) -> LsResult:
        assert path == "/views/abc"
        return LsResult(
            entries=[
                {"path": "/views/abc/frontend", "is_dir": True},
                {"path": "/views/abc/.manifest.json", "is_dir": False},
            ]
        )

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        assert paths == ["/views/abc/frontend/SKILL.md"]
        return [
            FileDownloadResponse(
                path=paths[0],
                content=b"---\nname: frontend\ndescription: frontend desc\n---\n\n# Frontend\n",
            )
        ]


def test_ruyi_skills_middleware_loads_metadata_from_configured_view() -> None:
    middleware = RuyiSkillsMiddleware(backend=BackendView())

    update = middleware.before_agent(
        {},
        runtime=None,  # type: ignore[arg-type]
        config={
            "configurable": {
                "skill_view_path": "/views/abc",
                "skill_view_hash": "abc",
            }
        },
    )

    assert update == {
        "ruyi_skills_view_hash": "abc",
        "skills_metadata": [
            {
                "name": "frontend",
                "description": "frontend desc",
                "path": "/views/abc/frontend/SKILL.md",
                "allowed_tools": [],
            }
        ],
    }
