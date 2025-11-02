
4. **Run the application**:
   ```bash
   python -m instagram_hashtags.hashtag
   ```
   Or use uvicorn:
   ```bash
   uvicorn file_convertor_webapp.ConverterWebApp:app --reload

   .venv/bin/python -m uvicorn instagram_hashtags.hashtag:app --reload --port 8000
   ```

5. **Run the worker** (for asynchronous conversions):
   ```bash
   .venv/bin/python -m instagram_hashtags.worker
   ```