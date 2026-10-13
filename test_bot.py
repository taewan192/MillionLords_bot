import asyncio
import importlib.util
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

spec = importlib.util.spec_from_file_location('tested_bot', Path(__file__).with_name('MillionLords_bot.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
logging.disable(logging.CRITICAL)


class PureTests(unittest.TestCase):
    def test_power_and_levels(self):
        self.assertEqual(m.find_max_level(m.parse_power_str('42.2T')), 149)
        self.assertEqual(m.parse_power_str('479ㅡ'), 479e6)
        self.assertEqual(m.apply_attack_percent(100, 535.75), 635.75)
        for value in ['infM', 'NaNM', '1e999P', '1e300P', '0M', '-2G']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                m.parse_power_str(value)
        for value in ['nan%', 'inf%', '-1%']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                m.parse_percent_str(value)
        with self.assertRaises(ValueError):
            m.apply_attack_percent(1e300, 1e300)

    def test_permission_checks_roles_not_user_ids(self):
        self.assertTrue(m._can_manage_teams(SimpleNamespace(id=m.Admin)))
        self.assertTrue(m._can_manage_teams(SimpleNamespace(id=123, roles=[SimpleNamespace(id=m.Builder)])))
        self.assertTrue(m._can_manage_teams(SimpleNamespace(id=123, roles=[SimpleNamespace(id=m.Captain)])))
        self.assertFalse(m._can_manage_teams(SimpleNamespace(id=m.Builder, roles=[])))
        self.assertFalse(m._can_manage_teams(SimpleNamespace(id=123)))

    def test_long_messages_including_single_long_line(self):
        for text in ['x' * 8000, 'ab\n' * 2500, 'x' * 1900, '\n' + 'x' * 5000]:
            chunks = list(m._message_chunks(text))
            self.assertTrue(all(0 < len(chunk) <= 1900 for chunk in chunks))
            self.assertEqual(''.join(chunks).replace('\n', ''), text.replace('\n', ''))

    def test_date_validation(self):
        self.assertEqual(m._canon_date_str_ymd('2026년10월13일'), '2026-10-13')
        self.assertIsNone(m._canon_date_str_ymd('2026-02-31'))

    def test_csv_formats_and_mutations(self):
        with tempfile.TemporaryDirectory() as tmp:
            teams, jewels = Path(tmp) / 'teams.csv', Path(tmp) / 'jewel.csv'
            with patch.object(m, 'TEAM_FILE', teams), patch.object(m, 'JEWEL_FILE', jewels), patch.object(m, '_team_cache', {'mtime': None, 'data': {}}):
                self.assertEqual(m._get_teams(), {})
                self.assertFalse(jewels.exists())
                self.assertTrue(m._add_member('가나다', '[KOR]')[0])
                self.assertTrue(m._add_member('Ab', 'kor')[0])
                self.assertFalse(m._add_member('AB', 'KOR')[0])
                self.assertEqual(m._rank_nicknames(m._get_teams(), 'Ab'), [('[KOR]', 'Ab')])
                self.assertTrue(m._add_member('Ab', 'Other')[0])
                self.assertTrue(m._delete_member('ab', 'KOR')[0])
                self.assertEqual(m._get_teams()['Other'], {'Ab'})
                self.assertTrue(m._delete_member('AB')[0])
                self.assertFalse(m._delete_member('unknown')[0])
                with patch.object(m, '_save_teams_to_csv', side_effect=PermissionError('locked')):
                    with self.assertRaises(PermissionError):
                        m._add_member('not-saved', 'Other')
                self.assertNotIn('not-saved', m._get_teams()['Other'])
                for encoding in ['utf-8-sig', 'utf-16', 'cp949']:
                    teams.write_text('team;nickname\n한국;가나다\n', encoding=encoding)
                    self.assertEqual(m._get_teams(force_reload=True), {'한국': {'가나다'}})
                    jewels.write_text('date,type\n2026-10-13,공격\n2026-10-23,공격\n', encoding=encoding)
                    self.assertEqual(m._lookup_jewel_value_by_date('2026-10-13'), (True, '공격'))
                    self.assertEqual(m._find_next_n_dates_by_type('공격', '2026-10-14'), (True, ['2026-10-23']))

    def test_token_loading_without_exposing_token(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(m, 'BASE_DIR', Path(tmp)), patch.dict(m.os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                m._load_token()
            (Path(tmp) / 'bot_token.txt').write_text('dummy-local-test', encoding='utf-8')
            self.assertEqual(m._load_token(), 'dummy-local-test')
            with patch.dict(m.os.environ, {'DISCORD_BOT_TOKEN': 'dummy-env-test'}):
                self.assertEqual(m._load_token(), 'dummy-env-test')

    def test_startup_error_is_saved_without_connecting(self):
        previous_level = logging.getLogger().level
        logging.disable(logging.NOTSET)
        try:
            with tempfile.TemporaryDirectory() as tmp, patch.object(m, 'LOG_DIR', Path(tmp)), patch.object(m, '_load_token', side_effect=RuntimeError('test-only-missing-token')), patch.object(m.bot, 'run') as run:
                self.assertEqual(m.main(), 1)
                run.assert_not_called()
                text = (Path(tmp) / 'runtime.log').read_text(encoding='utf-8')
                self.assertIn('test-only-missing-token', text)
                self.assertIn('Traceback', text)
        finally:
            logging.disable(logging.CRITICAL)
            logging.getLogger().setLevel(previous_level)

    def test_normal_exit_is_not_restarted(self):
        previous_level = logging.getLogger().level
        try:
            with tempfile.TemporaryDirectory() as tmp, patch.object(m, 'LOG_DIR', Path(tmp)), patch.object(m, '_load_token', return_value='dummy-test'), patch.object(m.bot, 'run') as run:
                self.assertEqual(m.main(), 0)
                run.assert_called_once_with('dummy-test', reconnect=True, log_handler=None)
        finally:
            logging.getLogger().setLevel(previous_level)


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        m._disk_log_queue = asyncio.Queue(maxsize=m.LOG_QUEUE_LIMIT)
        m._channel_log_queue = asyncio.Queue(maxsize=m.LOG_QUEUE_LIMIT)
        m._TEAM_LOCK = asyncio.Lock()

    async def test_backlog_is_bounded_without_creating_tasks(self):
        before = set(asyncio.all_tasks())
        for i in range(5000):
            m._emit_log(str(i) + 'x' * 15000, ['y' * 15000])
        self.assertEqual(m._channel_log_queue.qsize(), m.LOG_QUEUE_LIMIT)
        self.assertEqual(m._disk_log_queue.qsize(), m.LOG_QUEUE_LIMIT)
        self.assertEqual(before, set(asyncio.all_tasks()))
        while not m._disk_log_queue.empty():
            line, row = m._disk_log_queue.get_nowait()
            self.assertLessEqual(len(line), m.LOG_TEXT_LIMIT)
            self.assertLessEqual(len(row[0]), m.LOG_TEXT_LIMIT)
            m._disk_log_queue.task_done()
        await asyncio.wait_for(m._disk_log_queue.join(), 1)

    async def test_disk_failure_does_not_stop_worker(self):
        task = asyncio.create_task(m._disk_log_worker())
        with patch.object(m, '_write_file_log', side_effect=PermissionError('locked')), patch.object(m, '_write_month_csv_row') as csv_writer:
            m._emit_log('test', ['test'])
            await asyncio.wait_for(m._disk_log_queue.join(), 2)
            self.assertFalse(task.done())
            csv_writer.assert_called_once()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_channel_failures_retry_bounded_and_recover(self):
        channel = SimpleNamespace(send=AsyncMock(side_effect=[OSError('offline')] * 3 + [None]))
        fakebot = SimpleNamespace(wait_until_ready=AsyncMock(), get_channel=Mock(return_value=channel))
        with patch.object(m, 'bot', fakebot), patch.object(m.asyncio, 'sleep', new=AsyncMock()):
            task = asyncio.create_task(m._channel_log_worker())
            m._emit_log('failed entry')
            m._emit_log('recovered entry')
            await asyncio.wait_for(m._channel_log_queue.join(), 2)
            self.assertEqual(channel.send.await_count, 4)
            self.assertEqual(m._channel_log_queue.qsize(), 0)
            self.assertFalse(task.done())
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_reconnect_does_not_create_workers_or_sync_commands(self):
        fake = SimpleNamespace(user=SimpleNamespace(id=1))
        before = set(asyncio.all_tasks())
        with patch.object(m, 'bot', fake), patch.object(m.tree, 'sync', new=AsyncMock()) as sync:
            for _ in range(100):
                await m.on_ready()
            sync.assert_not_called()
        self.assertEqual(before, set(asyncio.all_tasks()))

    async def test_setup_and_close_worker_lifecycle(self):
        client = m.MillionLordsBot(command_prefix='!', intents=m.discord.Intents.none())
        with patch.object(m, 'bot', client), patch.object(client.tree, 'sync', new=AsyncMock(return_value=[])):
            # Use a pending event to model the pre-gateway wait without a network connection.
            ready = asyncio.Event()
            client.wait_until_ready = ready.wait
            await client.setup_hook()
            workers = client.log_workers[:]
            self.assertEqual(len(workers), 2)
            await client.close()
            self.assertTrue(all(task.done() for task in workers))
            self.assertEqual(client.log_workers, [])

    async def test_concurrent_team_updates(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(m, 'TEAM_FILE', Path(tmp) / 'teams.csv'), patch.object(m, '_team_cache', {'mtime': None, 'data': {}}):
            results = await asyncio.gather(*(m.add_member_async(f'Player{i}', 'KOR') for i in range(30)))
            self.assertTrue(all(ok for ok, _ in results))
            self.assertEqual(len(m._get_teams()['KOR']), 30)

    async def test_simulation_rejects_bad_input_and_handles_shortfall(self):
        def interaction():
            return SimpleNamespace(response=SimpleNamespace(defer=AsyncMock()), followup=SimpleNamespace(send=AsyncMock()),
                                   channel=SimpleNamespace(send=AsyncMock()), command=SimpleNamespace(name='lb'), user=SimpleNamespace())
        for troops, striker, multiplier, xp in [('infM', 100, 100, '1P'), ('1P', -1, 100, '1P'), ('1P', 100, 0, '1P'), ('1P', 100, 100, 'infP')]:
            inter = interaction()
            await asyncio.wait_for(m.run_simulation(inter, troops, striker, multiplier, xp, 0.9), 2)
            self.assertIn('입력값 오류', inter.followup.send.call_args.args[0])
        inter = interaction()
        with patch.object(m, 'log_command'):
            await m.run_simulation(inter, '4M', 0, 100, '100P', 0.9)
        embed = inter.followup.send.call_args.kwargs['embed']
        self.assertIn('도달하지 못했습니다', embed.fields[1].value)

    async def test_help_uses_supported_arguments_and_message_lengths(self):
        inter = SimpleNamespace(response=SimpleNamespace(defer=AsyncMock()), channel=SimpleNamespace(send=AsyncMock()),
                                edit_original_response=AsyncMock(), user=SimpleNamespace())
        with patch.object(m, 'log_command'):
            await m.show_help.callback(inter)
        self.assertTrue(inter.channel.send.called)
        for call in inter.channel.send.call_args_list:
            self.assertLessEqual(len(call.args[0]), 1900)
            self.assertEqual(call.kwargs['delete_after'], 180)


if __name__ == '__main__':
    unittest.main(verbosity=2)
