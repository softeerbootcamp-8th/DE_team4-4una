import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// 개발 중에는 vite가 :5173에, FastAPI가 :8501에 뜬다. 프론트엔드 코드는 항상
// 상대 경로로 /api를 부르고, 여기서 백엔드로 넘긴다 -- 그래야 빌드해서 FastAPI가
// 정적 파일로 서빙할 때와 같은 코드가 그대로 동작한다.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8501",
      "/_stcore": "http://127.0.0.1:8501",
    },
  },
});
