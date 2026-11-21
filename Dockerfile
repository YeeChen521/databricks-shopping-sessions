# Shopping Sessions: native C++ pipeline.
# Build stage compiles one static binary; the runtime image holds only that binary.
# No FROM line pins a platform: the scorers build natively on linux/amd64.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ss.cpp .
# x86-64-v3 = AVX2/BMI2/FMA (Haswell+); the scorers are Cascade Lake or newer.
RUN g++ -O3 -march=x86-64-v3 -std=c++17 -pthread -static -s -o /ss ss.cpp

# Nothing but the binary: /data, /output, /scratch and /tmp are mount points the runtime creates.
FROM scratch
COPY --from=build /ss /ss
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
USER 1000:1000
CMD ["/ss"]
