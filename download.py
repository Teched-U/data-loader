#!/usr/bin/env python3

"""
Utility script to download and parse files into a ready to train format

"""

import click
from typing import Dict, List, Any
import json
import asyncio
import os
import glob
import ffmpeg
import srt
import datetime
from pathlib import Path

COMMON_SKIP_DIRS: List[str] = ['.git']

def abspath(path: str) -> str:
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))

async def download_videos(config:Dict, video_file:str) -> None:
    """
    Download videos from joursera
    """

    # Parse config
    args = ['coursera-dl']
    for k, v in config.items():
        if k != "FLAGS":
            args.append(k)
            args.append(v)
        else:
            args += v

    # Get courses to download
    with open(video_file, 'r') as f:
        classes = f.readlines()
        args += [c.strip('\n') for c in classes]

        create = asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE
            )

        proc = await create

        while True:
            line = await proc.stdout.readline()
            print(line)
            if not line:
                break

        print('waiting for process to complete')
        await proc.wait()

        return_code = proc.returncode
        print('return code {}'.format(return_code))

async def ffmpeg_concat(videos: List[str], output_path: str) -> List[float]:
    # Could have used python-ffmpeg...but...
    """ffmpeg -i input1.mp4 -c copy -bsf:v h264_mp4toannexb -f mpegts intermediate1.ts"""

    # Intermediate results
    temp_files = []

    # Duration
    duration = []

    # Video lengths
    for video in videos:

        # Output for intermediate
        base_cmd = ['ffmpeg', '-i', video] +  '-c copy -bsf:v h264_mp4toannexb -f mpegts'.split()
        video_name = os.path.basename(video)
        temp_file = f"/tmp/{video_name}.ts"
        base_cmd += [temp_file]
        temp_files.append(temp_file)
        click.secho(' '.join(base_cmd), fg="green")
        base_cmd += ['-y']
        proc = await asyncio.create_subprocess_exec(
            *base_cmd,
            stdout=asyncio.subprocess.PIPE
        )
        await proc.wait()

        # Get video length
        duration.append(float(ffmpeg.probe(video)['format']['duration']))

    # Concate the intermediate results
    """ffmpeg -i "concat:intermediate1.ts|intermediate2.ts" -c copy -bsf:a aac_adtstoasc output.mp4"""
    base_cmd = []
    # Concate files
    concat_files = '|'.join(temp_files)
    concat = f'ffmpeg -i "concat:{concat_files}"'
    base_cmd += concat.split()

    # Output
    base_cmd += "-c copy -bsf:a aac_adtstoasc".split() + [output_path]

    # Override
    base_cmd += ['-y']
    click.secho(' '.join(base_cmd), fg="green")
    proc = await asyncio.create_subprocess_shell(
        ' '.join(base_cmd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    await proc.wait()

    return duration

async def srt_concat(srts: List[str], durations: List[float], output_path:str)-> None:
    offset = 0
    all_subs = []
    for srt_file, dur in zip(srts, durations):
        with open(srt_file, 'r') as f:
            subs = list(srt.parse(f))

            # Adjust timestamp
            for sub in subs:
                sub.start += datetime.timedelta(seconds=offset)
                sub.end += datetime.timedelta(seconds=offset)

            all_subs += subs
            offset += dur

    # Output adjusted srts to outputfile
    with open(output_path, 'w') as f:
        f.writelines(srt.compose(all_subs))


async def combine_module(mod_name: str, course_dir: str, output_dir: str) -> None:
    """  Combine one section's short videos into a longer one """
    print(mod_name)
    sections = next(os.walk(os.path.join(course_dir, mod_name)))[1]
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for sec in sections:
        mod_dir = os.path.join(course_dir, mod_name, sec)

        videos = glob.glob(f"{mod_dir}/*.mp4")
        videos.sort()

        # Combine videos
        durations = await ffmpeg_concat(videos, os.path.join(output_dir, f"{mod_name}.mp4"))

        # Combine subtitles
        srts = glob.glob(f"{mod_dir}/*.en.srt")
        srts.sort()
        await srt_concat(srts, durations, os.path.join(output_dir, f"{mod_name}.srt"))


async def concate_videos(config:Dict) -> None:
    base_dir = abspath(config["--path"])
    output_dir = abspath(config["output_path"])
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    classes = next(os.walk(base_dir))[1]
    for cl in classes:
        # Concate Video
        if cl in COMMON_SKIP_DIRS:
            continue
        output_class_dir = os.path.join(abspath(output_dir), cl)

        course_dir = os.path.join(base_dir, cl)
        modules = next(os.walk(course_dir))[1]

        for mod in modules:
            await combine_module(mod, course_dir, output_class_dir)



@click.command()
@click.option(
    '--mode',
    required=True,
    default='all',
    type=click.Choice(['get', 'concat', 'all'], case_sensitive=False),
    help="What specific operations to perform"
)
@click.option(
    '-c',
    '--config',
    'config_file',
    help='config file contains auth information'
)
@click.option(
    '-l',
    '--list',
    'video_list',
    help='list of videos (one for each line)'
)
def main(mode:str, config_file:str, video_list:str) ->None:
    config = {}
    with open(config_file, 'r') as f:
        config = json.load(f)

    if mode == 'get':
        click.echo("Getting Videos..")
        # Read user config
        asyncio.run(download_videos(config, video_list))
    elif mode == 'concat':
        asyncio.run(concate_videos(config))
    else:
        asyncio.run([
            download_videos(config, video_list),
            concate_videos(config)
        ])




if __name__ == '__main__':
    main()
